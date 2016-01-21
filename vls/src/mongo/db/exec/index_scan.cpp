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

#include "mongo/db/exec/index_scan.h"

#include "mongo/db/exec/filter.h"
#include "mongo/db/exec/working_set_computed_data.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/index/index_cursor.h"
#include "mongo/db/index/index_descriptor.h"

//#include <boost/timer.hpp>

namespace {

    // Return a value in the set {-1, 0, 1} to represent the sign of parameter i.
    int sgn(int i) {
        if (i == 0)
            return 0;
        return i > 0 ? 1 : -1;
    }

}  // namespace

namespace mongo {

    IndexScan::IndexScan(const IndexScanParams& params, WorkingSet* workingSet,
                         const MatchExpression* filter, bool use_chronos)
        : _workingSet(workingSet), _descriptor(params.descriptor), _hitEnd(false), _filter(filter), 
          _shouldDedup(params.descriptor->isMultikey()), _yieldMovedCursor(false), _params(params),
          _btreeCursor(NULL),
          _use_chronos(true),
          scanMask( 0x0 ),
          documentSetKeysInit( false ),
          sharedQueueScanDone( false ),
          _chronosExec(false),
          documentsRead(0),
          entriesAltered(0) {

        string amName;

        // If the query is using complex bounds, we must use a Btree access method, since that's the
        // only one that handles complex bounds.
        if (params.forceBtreeAccessMethod || !_params.bounds.isSimpleRange) {
            _iam = _descriptor->getIndexCatalog()->getBtreeIndex(_descriptor);
            amName = "";
        }
        else {
            amName = _descriptor->getIndexCatalog()->getAccessMethodName(_descriptor->keyPattern());
            _iam = _descriptor->getIndexCatalog()->getIndex(_descriptor);
        }

        if (IndexNames::GEO_2D == amName || IndexNames::GEO_2DSPHERE == amName) {
            // _endKey is meaningless for 2d and 2dsphere.
            verify(_params.bounds.isSimpleRange);
            verify(_params.bounds.endKey.isEmpty());
        }

        if (_params.doNotDedup) {
            _shouldDedup = false;
        }

        _specificStats.indexType = "BtreeCursor"; // TODO amName;
        _specificStats.indexName = _descriptor->infoObj()["name"].String();
        _specificStats.indexBounds = _params.bounds.toBSON();
        _specificStats.direction = _params.direction;
        _specificStats.isMultiKey = _descriptor->isMultikey();
        _specificStats.keyPattern = _descriptor->keyPattern();
        
        // --------- VLS --------- //
        
        req.tv_sec = 0;
        req.tv_nsec = 50000L;

        documentSetKeysIndex = -1;
        documentSetKeysSize = 0;
        //_scanId = 0;
        
        // getting database and collection
        database = cc().database();
        collection = database->getCollection( _params.ns );
        
        int scanSetVectorSize = (int)collection->statusMaskMapVector.size();
        for (int i = 0; i < scanSetVectorSize; i++)
            scanSetVector.push_back( scan_set(collection->max_id[i] + 1, 0) );
        
        if (( collection != NULL ) && ( database != NULL )) {
            if ( _use_chronos && collection->isUserCollection ) {
                
                // locking active mask
                activeMaskWriteLock w_lock(database->AMLock);
                
                // waiting for an available bit
                while (database->activeMaskFull) {
                    log() << "Waiting..." << endl;
                    database->activeMaskCondition.wait(w_lock);
                }
                
                //log() << "Bit is available!" << endl;
                
                uint64_t mask = 0x1;
                
                // finding a bit for the scan
                for (int n = 0; n < database->maxNumberScans; n++)
                {
                    if ( (database->activeMask & (mask << n)) != 0x0 )
                    {
                        //log() << "N: " << n << endl;
                        //log() << "Mask : " << std::hex << (~(mask << n)) << endl;
                        
                        // assigning scan to bit n
                        database->activeMask &= ~(mask << n);
                        database->indexActiveMask &= ~(mask << n);
                        collection->localActiveMask &= ~(mask << n);
                        collection->localIndexActiveMask &= ~(mask << n);
                        scanMask = (mask << n);
                        
                        // including scan set in collection
                        //_scanId = collection->indexScanId++;
                        //log() << "Scan ID: " << _scanId << endl;
                        //collection->indexScanMap.insert( std::make_pair(_scanId,
                        //        index_scan_info( _params.bounds, _descriptor->keyPattern(),
                        //                _params.direction, &scanSetVector )) );
                        
                        break;
                    }
                }
                
                // getting stable mask for that particular scan
                // this helps avoid reading the collection's stable mask
                {
                    stableMaskReadLock smr_lock(collection->SMLock);
                    scanStableMask = collection->stableMask & scanMask;
                }
                
                // if OR(Active Mask) is 0, there is no bit available
                // other scans should be aware of this
                if ( database->activeMask == 0x0 )
                    database->activeMaskFull = true;
                
                //log() << "Active Mask (1): " << std::hex << database->activeMask << endl;
                //log() << "Local Active Mask (1): " << std::hex << collection->localActiveMask << endl;
                //log() << "Stable Mask (1): " << std::hex << collection->stableMask << endl;
                //log() << "Scan Mask : " << std::hex << scanMask << endl;
                
            } else {
                _use_chronos = false;
            }
        } else {
            _use_chronos = false;
        }
        
        //log() << "Index Scan Initialized!" << endl;
        
        // --------- VLS --------- //
    }

    void IndexScan::initIndexCursor() {
        CursorOptions cursorOptions;

        if (1 == _params.direction) {
            cursorOptions.direction = CursorOptions::INCREASING;
        }
        else {
            cursorOptions.direction = CursorOptions::DECREASING;
        }

        IndexCursor *cursor;
        Status s = _iam->newCursor(&cursor, scanStableMask, scanMask);
        verify(s.isOK());
        _indexCursor.reset(cursor);
        _indexCursor->setOptions(cursorOptions);
        if ( _use_chronos )
            _indexCursor->setChronosParameters(&(collection->statusMaskMapVector),
                                               &(collection->ofsMap),
                                               &scanSetVector,
                                               collection->getIndexCatalog(),
                                               _descriptor->getIndexNumber(),
                                               entriesAltered);

        if (_params.bounds.isSimpleRange) {
            // Start at one key, end at another.
            Status status = _indexCursor->seek(_params.bounds.startKey);
            if (!status.isOK()) {
                warning() << "Seek failed: " << status.toString();
                _hitEnd = true;
            }
            if (!isEOF()) {
                _specificStats.keysExamined = 1;
            }
        }
        else {
            // "Fast" Btree-specific navigation.
            _btreeCursor = static_cast<BtreeIndexCursor*>(_indexCursor.get());
            _checker.reset(new IndexBoundsChecker(&_params.bounds,
                                                  _descriptor->keyPattern(),
                                                  _params.direction));

            int nFields = _descriptor->keyPattern().nFields();
            vector<const BSONElement*> key;
            vector<bool> inc;
            key.resize(nFields);
            inc.resize(nFields);
            if (_checker->getStartKey(&key, &inc)) {
                _btreeCursor->seek(key, inc);
                _keyElts.resize(nFields);
                _keyEltsInc.resize(nFields);
            }
            else {
                _hitEnd = true;
            }
        }
    }
    
    void IndexScan::resetStatusMaskBits() {
        
        //boost::timer t;
        
        //uint64_t statusMask = 0x0;
        //bool success = false;
        
        bool stableIsZero = (scanStableMask == 0x0) ? true : false;
        
        for (int i = 0; i < (int)collection->statusMaskMapVector.size(); i++)
        {
            st_mask_map* statusMaskMap = &(collection->statusMaskMapVector[i]);
            for( st_mask_map::iterator it = statusMaskMap->begin();
                    it != statusMaskMap->end(); it++)
            {
                
                if ( stableIsZero )
                    (it)->fetch_and_store( scanMask | (*it) );
                else
                    (it)->fetch_and_store( ~scanMask & (*it) );
                
                /*success = false;
                while ( !success )
                {
                    statusMask = *it;
                    
                    // update status mask
                    if ( ( ( scanStableMask ^ statusMask ) & scanMask ) == 0x0 )
                        success = ( (it)->compare_and_swap( ~( ( ~statusMask ) ^ scanMask ), statusMask ) == statusMask ) ? true : false;
                    else
                        success = true;
                }*/
            }
        }
        
        // experimental purposes
        //debug_str << "Time Resetting (" << _scanId << "): " << std::setprecision(15) << t.elapsed() << endl;
    }
    
    void IndexScan::resetIndexMaskBits(int idx_number)
    {
        //log() << "Resetting Index Masks..." << endl;
        
        idx_status zero (0x0);
        IndexCatalog* indexCatalog = collection->getIndexCatalog();
        
        for (int a = 0; a < (int)indexCatalog->indexMaskMap[idx_number].size(); a++)
        {
            idx_mask_map* _indexMaskMap = &(indexCatalog->indexMaskMap[idx_number][a]);
            
            idx_mask_map::iterator it = _indexMaskMap->begin();
            idx_status_map::iterator status_it = ( &(indexCatalog->indexStatusMap[idx_number][a]) )->begin();
            ID id = 0;
            
            while ( it != _indexMaskMap->end() )
            {
                {
                    //indexMaskWriteLock imw_lock(collection->IMLock);
                
                    if ( (*status_it) == zero )
                    {
                        //log() << "Resetting Insert!" << endl;
                        (it)->fetch_and_store( ~scanMask & (*it) );
                    }
                    else
                    {
                        //log() << "Resetting Remove!" << endl;
                        (it)->fetch_and_store( scanMask | (*it) );
                        
                        if ( (*it) == 0xFFFFFFFFFFFFFFFF )
                        {
                            // unindex corresponding record
                            // need to get corresponding record (in shared queue)
                            doc_set::const_accessor const_a;
                            bool find = collection->documentSet.find(const_a, indexCatalog->queueMap[idx_number][id]);
                            
                            if ( find )
                            {
                                //log() << "Unindexing record with id " << (const_a->second).document.getField("_id").toInt() << endl;
                                
                                // here, we should unindex for real, but due to MongoDB locking mechanism,
                                // we skip this for now, and add a timer that reflects the process
                                /*indexCatalog->unindexRecordChronos( idx_number,
                                        (const_a->second).document,
                                        DiskLoc(a, collection->idOfsMap[a][id]),
                                        false );*/
                                nanosleep(&req, (struct timespec *)NULL);
                                
                                indexCatalog->queueMap[idx_number].erase(id);
                            }
                            else
                                log() << "Oops... something is wrong here..." << endl;
                            
                            // resetting bit -- in comments because we are not unindexing right now
                            // (*status_it) = zero;
                        }
                    }
                    
                } // lock
                
                // iterators and id
                status_it++;
                it++;
                id++;
            }
        }
    }
    
    bool IndexScan::chronosNextObj(BSONObj &nextObj) {
            
        bool skip = false;
        
        if ( !documentSetKeysInit ) {
            
            // do not add records to record set anymore
            //collection->indexScanMap.erase( _scanId );
            
            // resetting local active masks and stable mask
            {
                activeMaskWriteLock amw_lock(database->AMLock);
                {
                    stableMaskWriteLock smw_lock(collection->SMLock);
                    
                    collection->localActiveMask |= scanMask;
                    collection->localIndexActiveMask |= scanMask;
                    collection->stableMask = ~( ( ~collection->stableMask ) ^ scanMask );
                }
            }
            
            // resetting index masks
            //if (entriesAltered > 0)
            //    resetIndexMaskBits( _descriptor->getIndexNumber() );
            
            // resetting all status mask map for this scan
            // the idea is for all documents to look "already read" by the scan
            // this avoids more updates to put records in the queue for this scan
            //if (documentsRead < collection->numRecords())
            //    resetStatusMaskBits();
            
            // "materializing" the document set for this particular scan
             // since this scan will not need new documents added to the set
            if ( collection->documentSetSize == 0 )
                sharedQueueScanDone = true;
            else
            {
                {
                    // locking document set
                    documentSetWriteLock dsw_lock(collection->DSLock);
                    
                    for( doc_set::const_iterator it = collection->documentSet.begin();
                            it != collection->documentSet.end(); ++it )
                        documentSetKeys.push_back(it->first);
                }
    
                documentSetKeysInit = true;
                documentSetKeysSize = documentSetKeys.size();
            }

        }
        
        // if scan in shared queue is still not over...
        if ( !sharedQueueScanDone ) {

            //boost::timer t;
            
            bool nextDocument = true;
            bool find = false;
            queue_document queueDocument;
            uint64_t queueStatusMask = 0x0;
            ID id;
            int _a;
            
            while ( nextDocument ) {
                
                documentSetKeysIndex += 1;
                
                if ( (documentSetKeysIndex + 1) <= documentSetKeysSize ) {

                    doc_set::accessor a;
                    find = collection->documentSet.find(a, documentSetKeys[documentSetKeysIndex]);

                    // document has not been removed from shared queue
                    if ( find ) {
                        
                        // getting the document from the shared queue
                        queueDocument = a->second;
                        queueStatusMask = queueDocument.queueStatusMask;
                        id = queueDocument.id;
                        _a = queueDocument._a;
                        
                        // if OR(Status) is 0, read the document from the queue
                        // status = ( scan mask & queue status mask )
                        if ( ( scanMask & queueStatusMask ) == 0x0 ) {
                            
                            if ( scanSetVector[_a][id] == 1 ) {
                            
                                // read document
                                nextDocument = false;
                                skip = true;
        
                                nextObj = queueDocument.document;
                                
                            }
                                
                            // computing new Queue Status Mask
                            queueStatusMask |= scanMask;
                            
                            // if AND(New Queue Status Mask) is 1
                            // document can be removed from queue
                            if ( queueStatusMask == 0xFFFFFFFFFFFFFFFF ) {
                                {
                                    // locking document set -- prevent concurrency with traversal
                                    documentSetWriteLock dsw_lock(collection->DSLock);
                                    collection->documentSet.erase(a);
                                }
                                collection->documentSetSize -= 1;
                                
                                //log() << "Removing " << queueDocument.document.getField("_id").toInt() << endl;
                                //log() << "Size of queue: " << collection->documentSet.size() << endl;
                            }
                            
                            // if AND(New Queue Status Mask) is 0
                            // update Queue Status Mask
                            else {
                                queueDocument.queueStatusMask = queueStatusMask;
                                
                                //log() << "Queue Mask: " << queueStatusMask << endl;
                                
                                // updating document
                                a->second = queueDocument;
                            }
                            
                        }
                        
                    }
                    
                } else {
                    nextDocument = false;
                }
                
            } // while

            // experimental purposes
            //debug_str << "Time Queue (" << _scanId << "): " << std::setprecision(15) << t.elapsed() << endl;

        } // if
        
        if ( !skip )
        {
            // no more documents to be read by the scan
            // free the bits
            if ( (documentSetKeysIndex + 1) >= documentSetKeysSize ) {
                
                // updating Active Mask and Stable Mask
                // note that Stable Mask can be updated before scanning the queue
                /*{
                    activeMaskWriteLock amw_lock(database->AMLock);
                    
                    {
                        stableMaskWriteLock smw_lock(collection->SMLock);
                        
                        database->activeMask |= scanMask;
                        database->indexActiveMask |= scanMask;
                        collection->stableMask = ~( ( ~collection->stableMask ) ^ scanMask );
                        
                        //log() << "Active Mask (2): " << std::hex << database->activeMask << endl;
                        //log() << "Stable Mask (2): " << std::hex << collection->stableMask << endl;
                        
                        // allow an awaiting scan to start
                        database->activeMaskFull = false;
                        
                        // experimental purposes
                        //if (database->activeMask == 0xFFFFFFFFFFFFFFFF)
                        //{
                        //    std::ofstream out("/home/vgc/fchirigati/chronos/exp/testdb_10m/mongodb-chronos/tbb_stat_index_updates_debug.out");
                        //    out << debug_str.str();
                        //    out.close();
                        //}
                        
                    } // releasing stable mask lock 
                    
                } // releasing active mask lock
                
                database->activeMaskCondition.notify_one();*/
                
                FlipPhase* flip = new FlipPhase(database, collection,
                        scanStableMask, scanMask);
                flip->go();
    
            }
            
        }
        
        return skip;

    }

    PlanStage::StageState IndexScan::work(WorkingSetID* out) {
        ++_commonStats.works;

        if (NULL == _indexCursor.get()) {
            // First call to work().  Perform cursor init.
            initIndexCursor();
            checkEnd();
            if ( _use_chronos )
                _indexCursor->verifyDocument();
        }
        else if (_yieldMovedCursor) {
            _yieldMovedCursor = false;
            // Note that we're not calling next() here.  We got the next thing when we recovered
            // from yielding.
        }
        
        if ( _indexCursor->isInvalid() )
        {
            // record is in document set -- current version was not read
            _indexCursor->next();
            checkEnd();
            
            documentsRead += 1;
            
            ++_commonStats.needTime;
            return PlanStage::NEED_TIME;
        }
        
        DiskLoc loc;
        bool chronos = false;
        
        if ( _chronosExec || isEOF() )
        {
            if ( _use_chronos ) {
                //log() << "Chronos" << endl;
                loc = *(new DiskLoc(-2, 0)); //invalid
                chronos = chronosNextObj(_ownedKeyObj);
            }
            
            if ( !chronos )
                return PlanStage::IS_EOF;
        }
        else
        {
            // Grab the next (key, value) from the index.
            _ownedKeyObj = _indexCursor->getKey().getOwned();
            if ( _use_chronos )
                loc = _indexCursor->getCurrentLoc();
            else
                loc = _indexCursor->getValue();

            // Move to the next result.
            // The underlying IndexCursor points at the *next* thing we want to return.  We do this so
            // that if we're scanning an index looking for docs to delete we don't continually clobber
            // the thing we're pointing at.
            _indexCursor->next();
            checkEnd();

            if (_shouldDedup) {
                ++_specificStats.dupsTested;
                if (_returned.end() != _returned.find(loc)) {
                    ++_specificStats.dupsDropped;
                    ++_commonStats.needTime;
                    return PlanStage::NEED_TIME;
                }
                else {
                    _returned.insert(loc);
                }
            }
        }

        WorkingSetID id = _workingSet->allocate();
        WorkingSetMember* member = _workingSet->get(id);
        member->loc = loc;
        if ( chronos ) {
            member->obj = _ownedKeyObj;
            member->state = WorkingSetMember::LOC_AND_UNOWNED_OBJ;
        } else {
            documentsRead += 1;
            member->keyData.push_back(IndexKeyDatum(_descriptor->keyPattern(), _ownedKeyObj));
            member->state = WorkingSetMember::LOC_AND_IDX;
        }

        if (Filter::passes(member, _filter)) {
            if (NULL != _filter) {
                ++_specificStats.matchTested;
            }
            if ( _params.addKeyMetadata ) {
                BSONObjBuilder bob;
                bob.appendKeys(_descriptor->keyPattern(), _ownedKeyObj);
                member->addComputed(new IndexKeyComputedData(bob.obj()));
            }
            *out = id;
            ++_commonStats.advanced;
            return PlanStage::ADVANCED;
        }

        _workingSet->free(id);
        ++_commonStats.needTime;
        return PlanStage::NEED_TIME;
    }
    
    bool IndexScan::isChronosExec() {
        return _chronosExec;
    }

    bool IndexScan::isEOF() {
        if (NULL == _indexCursor.get()) {
            // Have to call work() at least once.
            return false;
        }

        // If there's a limit on how many keys we can scan, we may be EOF when we hit that.
        if (0 != _params.maxScan) {
            if (_specificStats.keysExamined >= _params.maxScan) {
                if ( _use_chronos ) _chronosExec = true;
                return true;
            }
        }

        return _hitEnd || _indexCursor->isEOF();
    }

    void IndexScan::prepareToYield() {
        ++_commonStats.yields;

        if (isEOF() || (NULL == _indexCursor.get())) { return; }
        if ( _chronosExec ) { return; }
        
        _savedKey = _indexCursor->getKey().getOwned();
        _savedLoc = _indexCursor->getValue();
        _indexCursor->savePosition();
    }

    void IndexScan::recoverFromYield() {
        ++_commonStats.unyields;

        if (isEOF() || (NULL == _indexCursor.get())) {
            return;
        }
        
        if ( _chronosExec ) { return; }

        // We can have a valid position before we check isEOF(), restore the position, and then be
        // EOF upon restore.
        if (!_indexCursor->restorePosition().isOK() || _indexCursor->isEOF()) {
            _hitEnd = true;
            if ( _use_chronos ) _chronosExec = true;
            return;
        }

        if (!_savedKey.binaryEqual(_indexCursor->getKey())
            || _savedLoc != _indexCursor->getValue()) {
            // Our restored position isn't the same as the saved position.  When we call work()
            // again we want to return where we currently point, not past it.
            _yieldMovedCursor = true;

            ++_specificStats.yieldMovedCursor;

            // Our restored position might be past endKey, see if we've hit the end.
            checkEnd();
        }
    }

    void IndexScan::invalidate(const DiskLoc& dl) {
        ++_commonStats.invalidates;

        // If we see this DiskLoc again, it may not be the same doc. it was before, so we want to
        // return it.
        unordered_set<DiskLoc, DiskLoc::Hasher>::iterator it = _returned.find(dl);
        if (it != _returned.end()) {
            ++_specificStats.seenInvalidated;
            _returned.erase(it);
        }
    }

    void IndexScan::checkEnd() {
        if ( _indexCursor->isChronosExec() ) {
            _chronosExec = true;
            return;
        }
        
        if (isEOF()) {
            _commonStats.isEOF = true;
            return;
        }

        if (_params.bounds.isSimpleRange) {
            // "Normal" start -> end scanning.
            verify(NULL == _btreeCursor);
            verify(NULL == _checker.get());

            // If there is an empty endKey we will scan until we run out of index to scan over.
            if (_params.bounds.endKey.isEmpty()) { return; }

            int cmp = sgn(_params.bounds.endKey.woCompare(_indexCursor->getKey(),
                _descriptor->keyPattern()));

            if ((cmp != 0 && cmp != _params.direction)
                || (cmp == 0 && !_params.bounds.endKeyInclusive)) {

                _hitEnd = true;
                _commonStats.isEOF = true;
                if ( _use_chronos ) _chronosExec = true;
            }

            if (!isEOF() && _params.bounds.isSimpleRange) {
                ++_specificStats.keysExamined;
            }
        }
        else {
            verify(NULL != _btreeCursor);
            verify(NULL != _checker.get());

            // Use _checker to see how things are.
            for (;;) {
                //cout << "current index key is " << _indexCursor->getKey().toString() << endl;
                //cout << "keysExamined is " << _specificStats.keysExamined << endl;
                IndexBoundsChecker::KeyState keyState;
                keyState = _checker->checkKey(_indexCursor->getKey(),
                                              &_keyEltsToUse,
                                              &_movePastKeyElts,
                                              &_keyElts,
                                              &_keyEltsInc);

                if (IndexBoundsChecker::DONE == keyState) {
                    _hitEnd = true;
                    if ( _use_chronos ) _chronosExec = true;
                    break;
                }

                // This seems weird but it's the old definition of nscanned.
                ++_specificStats.keysExamined;

                if (IndexBoundsChecker::VALID == keyState) {
                    break;
                }

                //cout << "skipping...\n";
                verify(IndexBoundsChecker::MUST_ADVANCE == keyState);
                _btreeCursor->skip(_indexCursor->getKey(), _keyEltsToUse, _movePastKeyElts,
                                   _keyElts, _keyEltsInc);

                // Must check underlying cursor EOF after every cursor movement.
                if (_btreeCursor->isEOF()) {
                    _hitEnd = true;
                    if ( _use_chronos ) _chronosExec = true;
                    break;
                }

                // TODO: Can we do too much scanning here?  Old BtreeCursor stops scanning after a
                // while and relies on a Matcher to make sure the result is ok.
            }
        }
    }

    PlanStageStats* IndexScan::getStats() {
        _commonStats.isEOF = isEOF();
        auto_ptr<PlanStageStats> ret(new PlanStageStats(_commonStats, STAGE_IXSCAN));
        ret->specific.reset(new IndexScanStats(_specificStats));
        return ret.release();
    }

}  // namespace mongo
