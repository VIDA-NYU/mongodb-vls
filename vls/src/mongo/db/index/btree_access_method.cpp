/**
*    Copyright (C) 2016, New York University
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

/**
*    Copyright (C) 2013 10gen Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#include "mongo/db/index/btree_access_method.h"

#include <vector>

#include "mongo/base/status.h"
#include "mongo/db/index/btree_index_cursor.h"
#include "mongo/db/index/btree_interface.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/pdfile.h"
#include "mongo/db/pdfile_private.h"
#include "mongo/db/structure/btree/state-inl.h"

namespace mongo {

    BtreeBasedAccessMethod::BtreeBasedAccessMethod(BtreeInMemoryState* btreeState)
        : _btreeState(btreeState), _descriptor(btreeState->descriptor()) {

        verify(0 == _descriptor->version() || 1 == _descriptor->version());
        _interface = BtreeInterface::interfaces[_descriptor->version()];
        
        BSONObjIterator it(_descriptor->keyPattern());
        while (it.more()) {
            BSONElement elt = it.next();
            fieldNames.push_back(elt.fieldName());
            fixed.push_back(BSONElement());
        }
    }

    // Find the keys for obj, put them in the tree pointing to loc
    Status BtreeBasedAccessMethod::insert(const BSONObj& obj, const DiskLoc& loc,
            const InsertDeleteOptions& options, int64_t* numInserted) {

        *numInserted = 0;

        BSONObjSet keys;
        // Delegate to the subclass.
        getKeys(obj, &keys);

        Status ret = Status::OK();

        for (BSONObjSet::const_iterator i = keys.begin(); i != keys.end(); ++i) {
            try {
                _interface->bt_insert(_btreeState.get(),
                                      _btreeState->head(),
                                      loc,
                                      *i,
                                      options.dupsAllowed,
                                      true);
                ++*numInserted;
            } catch (AssertionException& e) {
                if (10287 == e.getCode() && _descriptor->isBackgroundIndex()) {
                    // This is the duplicate key exception.  We ignore it for some reason in BG
                    // indexing.
                    DEV log() << "info: key already in index during bg indexing (ok)\n";
                } else if (!options.dupsAllowed) {
                    // Assuming it's a duplicate key exception.  Clean up any inserted keys.
                    for (BSONObjSet::const_iterator j = keys.begin(); j != i; ++j) {
                        removeOneKey(*j, loc, "");
                    }
                    *numInserted = 0;
                    return Status(ErrorCodes::DuplicateKey, e.what(), e.getCode());
                } else {
                    problem() << " caught assertion addKeysToIndex "
                              << _descriptor->indexNamespace()
                              << obj["_id"] << endl;
                    ret = Status(ErrorCodes::InternalError, e.what(), e.getCode());
                }
            }
        }

        if (*numInserted > 1) {
            _btreeState->setMultikey();
        }

        return ret;
    }

    bool BtreeBasedAccessMethod::removeOneKey(const BSONObj& key, const DiskLoc& loc, const char* fieldName) {
        bool ret = false;

        try {
            ret = _interface->unindex(_btreeState.get(),
                                      _btreeState->head(),
                                      key,
                                      loc);
            //addRemovedKey(loc, key, fieldName);
        } catch (AssertionException& e) {
            problem() << "Assertion failure: _unindex failed "
                << _descriptor->indexNamespace() << endl;
            out() << "Assertion failure: _unindex failed: " << e.what() << '\n';
            out() << "  obj:" << loc.obj().toString() << '\n';
            out() << "  key:" << key.toString() << '\n';
            out() << "  dl:" << loc.toString() << endl;
            logContext();
        }

        return ret;
    }

    // Remove the provided doc from the index.
    Status BtreeBasedAccessMethod::remove(const BSONObj &obj, const DiskLoc& loc,
        const InsertDeleteOptions &options, int64_t* numDeleted) {

        BSONObjSet keys;
        getKeys(obj, &keys);
        *numDeleted = 0;

        unsigned field_name_i = 0;
        for (BSONObjSet::const_iterator i = keys.begin(); i != keys.end(); ++i) {
            bool thisKeyOK = removeOneKey(*i, loc, fieldNames[ field_name_i ]);
            field_name_i++;

            if (thisKeyOK) {
                ++*numDeleted;
            } else if (options.logIfError) {
                log() << "unindex failed (key too big?) " << _descriptor->indexNamespace()
                      << " key: " << *i << " " << loc.obj()["_id"] << endl;
            }
        }

        return Status::OK();
    }

    // Return keys in l that are not in r.
    // Lifted basically verbatim from elsewhere.
    static void setDifference(const BSONObjSet &l, const BSONObjSet &r, vector<BSONObj*> *diff) {
        // l and r must use the same ordering spec.
        verify(l.key_comp().order() == r.key_comp().order());
        BSONObjSet::const_iterator i = l.begin();
        BSONObjSet::const_iterator j = r.begin();
        while ( 1 ) {
            if ( i == l.end() )
                break;
            while ( j != r.end() && j->woCompare( *i ) < 0 )
                j++;
            if ( j == r.end() || i->woCompare(*j) != 0  ) {
                const BSONObj *jo = &*i;
                diff->push_back( (BSONObj *) jo );
            }
            i++;
        }
    }

    Status BtreeBasedAccessMethod::touch(const BSONObj& obj) {
        BSONObjSet keys;
        getKeys(obj, &keys);

        for (BSONObjSet::const_iterator i = keys.begin(); i != keys.end(); ++i) {
            int unusedPos;
            bool unusedFound;
            DiskLoc unusedDiskLoc;
            _interface->locate(_btreeState.get(),
                               _btreeState->head(),
                               *i,
                               unusedPos,
                               unusedFound,
                               unusedDiskLoc,
                               1);
        }

        return Status::OK();
    }

    DiskLoc BtreeBasedAccessMethod::findSingle( const BSONObj& key ) {
        if ( 0 == _descriptor->version() ) {
            return _btreeState->getHeadBucket<V0>()->findSingle( _btreeState.get(),
                                                                 _btreeState->head(),
                                                                 key );
        }
        if ( 1 == _descriptor->version() ) {
            return _btreeState->getHeadBucket<V1>()->findSingle( _btreeState.get(),
                                                                 _btreeState->head(),
                                                                 key );
        }
        verify( 0 );
    }


    Status BtreeBasedAccessMethod::validate(int64_t* numKeys) {
        *numKeys = _interface->fullValidate(_btreeState.get(),
                                            _btreeState->head(),
                                            _descriptor->keyPattern());
        return Status::OK();
    }

    Status BtreeBasedAccessMethod::validateUpdate(
        const BSONObj &from, const BSONObj &to, const DiskLoc &record,
        const InsertDeleteOptions &options, UpdateTicket* status) {

        BtreeBasedPrivateUpdateData *data = new BtreeBasedPrivateUpdateData();
        status->_indexSpecificUpdateData.reset(data);

        getKeys(from, &data->oldKeys);
        getKeys(to, &data->newKeys);
        data->loc = record;
        data->dupsAllowed = options.dupsAllowed;

        setDifference(data->oldKeys, data->newKeys, &data->removed);
        setDifference(data->newKeys, data->oldKeys, &data->added);

        bool checkForDups = !data->added.empty()
            && (KeyPattern::isIdKeyPattern(_descriptor->keyPattern()) || _descriptor->unique())
            && !options.dupsAllowed;

        if (checkForDups) {
            for (vector<BSONObj*>::iterator i = data->added.begin(); i != data->added.end(); i++) {
                if (_interface->wouldCreateDup(_btreeState.get(),
                                               _btreeState->head(),
                                               **i, record)) {
                    status->_isValid = false;
                    return Status(ErrorCodes::DuplicateKey,
                                  _interface->dupKeyError(_btreeState.get(),
                                                          _btreeState->head(),
                                                          **i));
                }
            }
        }

        status->_isValid = true;

        return Status::OK();
    }

    Status BtreeBasedAccessMethod::update(const UpdateTicket& ticket, int64_t* numUpdated) {
        if (!ticket._isValid) {
            return Status(ErrorCodes::InternalError, "Invalid updateticket in update");
        }

        BtreeBasedPrivateUpdateData* data =
            static_cast<BtreeBasedPrivateUpdateData*>(ticket._indexSpecificUpdateData.get());

        if (data->oldKeys.size() + data->added.size() - data->removed.size() > 1) {
            _btreeState->setMultikey();
        }

        for (size_t i = 0; i < data->added.size(); ++i) {
            _interface->bt_insert(_btreeState.get(),
                                  _btreeState->head(),
                                  data->loc,
                                  *data->added[i],
                                  data->dupsAllowed,
                                  true);
        }

        unsigned field_name_i = 0;
        for (size_t i = 0; i < data->removed.size(); ++i) {
            _interface->unindex(_btreeState.get(),
                                _btreeState->head(),
                                *data->removed[i],
                                data->loc);
            //addRemovedKey(data->loc, *data->removed[i], fieldNames[ field_name_i ]);
            field_name_i++;
        }

        *numUpdated = data->added.size();

        return Status::OK();
    }

    // Standard Btree implementation below.
    BtreeAccessMethod::BtreeAccessMethod(BtreeInMemoryState* btreeState)
        : BtreeBasedAccessMethod(btreeState) {

        if (0 == _descriptor->version()) {
            _keyGenerator.reset(new BtreeKeyGeneratorV0(fieldNames, fixed,
                _descriptor->isSparse()));
        } else if (1 == _descriptor->version()) {
            _keyGenerator.reset(new BtreeKeyGeneratorV1(fieldNames, fixed,
                _descriptor->isSparse()));
        } else {
            massert(16745, "Invalid index version for key generation.", false );
        }
        
        _collection = NULL;
    }
    
    void BtreeAccessMethod::addRemovedKey(const DiskLoc& loc, const BSONObj& key, const char* fieldName)
    {
        /*if (_collection != NULL)
        {
            if ((_collection->indexScanMap.size() > 0) && (strlen(fieldName) != 0) )
            {
                BSONObjBuilder b;
                b.appendAs(key.getField(""), fieldName);
                BSONObj obj = b.obj();
                
                int _a = loc.a();
                uint32_t loc_id = loc.id(_collection->ofsMap[_a]);
                
                boost::unordered_map< int, index_scan_info >::iterator it;
                for (it = _collection->indexScanMap.begin(); it != _collection->indexScanMap.end(); it++)
                {
                    IndexBoundsChecker checker = IndexBoundsChecker(&((it->second).bounds),
                            (it->second).keyPattern, (it->second).direction);
                    if ( checker.isValidKey(obj) ) {
                        //log() << "Saving " << key << " (id=" << loc_id << ")" << endl;
                        ( ((it->second).scanSetVector)->at(_a) )[loc_id] = 1;
                    //} else {
                    //    log() << "DO NOT SAVE " << key << " (id=" << loc_id << ")" << endl;
                    //}
                }
            }
        }*/
    }

    void BtreeAccessMethod::getKeys(const BSONObj& obj, BSONObjSet* keys) {
        _keyGenerator->getKeys(obj, keys);
    }
    
    void BtreeAccessMethod::setCollection(Collection *collection)
    {
        _collection = collection;
    }
    
    Status BtreeAccessMethod::newCursor(IndexCursor** out) const {
        return Status::OK();
    }

    Status BtreeAccessMethod::newCursor(IndexCursor** out, uint64_t &scanStableMask, uint64_t &scanMask) const {
        *out = new BtreeIndexCursor(_btreeState.get(), _interface, scanStableMask, scanMask);
        return Status::OK();
    }

}  // namespace mongo
