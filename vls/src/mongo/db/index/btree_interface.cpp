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

#include "mongo/db/structure/btree/btree.h"
#include "mongo/db/structure/btree/state.h"
#include "mongo/db/structure/btree/state-inl.h"
#include "mongo/db/index/btree_interface.h"
#include "mongo/db/pdfile.h"

namespace mongo {

    template <class Version>
    class BtreeInterfaceImpl : public BtreeInterface {
    public:
        // typedef typename BucketBasics<Version>::VersionNode VersionNode;

        virtual ~BtreeInterfaceImpl() { }

        virtual int bt_insert(BtreeInMemoryState* btreeState,
                              const DiskLoc thisLoc,
                              const DiskLoc recordLoc,
                              const BSONObj& key,
                              bool dupsallowed,
                              bool toplevel) {
            // FYI: toplevel has a default value of true in btree.h
            return btreeState->getBucket<Version>(thisLoc)->bt_insert(btreeState,
                                                                      thisLoc,
                                                                      recordLoc,
                                                                      key,
                                                                      dupsallowed,
                                                                      toplevel);
        }

        virtual bool unindex(BtreeInMemoryState* btreeState,
                             const DiskLoc thisLoc,
                             const BSONObj& key,
                             const DiskLoc recordLoc) {
            return btreeState->getBucket<Version>(thisLoc)->unindex(btreeState,
                                                                    thisLoc,
                                                                    key,
                                                                    recordLoc);
        }

        virtual DiskLoc locate(const BtreeInMemoryState* btreeState,
                               const DiskLoc& thisLoc,
                               const BSONObj& key,
                               int& pos,
                               bool& found,
                               const DiskLoc& recordLoc,
                               int direction) const {
            // FYI: direction has a default of 1
            return btreeState->getBucket<Version>(thisLoc)->locate(btreeState,
                                                                   thisLoc,
                                                                   key,
                                                                   pos,
                                                                   found,
                                                                   recordLoc,
                                                                   direction);
        }

        virtual bool wouldCreateDup(const BtreeInMemoryState* btreeState,
                                    const DiskLoc& thisLoc,
                                    const BSONObj& key,
                                    const DiskLoc& self) const {
            typename Version::KeyOwned ownedVersion(key);
            return btreeState->getBucket<Version>(thisLoc)->wouldCreateDup(btreeState,
                                                                           thisLoc,
                                                                           ownedVersion,
                                                                           self);
        }

        virtual void customLocate(const BtreeInMemoryState* btreeState,
                                  DiskLoc& locInOut,
                                  int& keyOfs,
                                  const BSONObj& keyBegin,
                                  int keyBeginLen, bool afterVersion,
                                  const vector<const BSONElement*>& keyEnd,
                                  const vector<bool>& keyEndInclusive,
                                  int direction,
                                  pair<DiskLoc, int>& bestParent) const {
            return BtreeBucket<Version>::customLocate(btreeState,
                                                      locInOut,
                                                      keyOfs,
                                                      keyBegin,
                                                      keyBeginLen, afterVersion,
                                                      keyEnd,
                                                      keyEndInclusive,
                                                      direction,
                                                      bestParent);
        }

        virtual void advanceTo(const BtreeInMemoryState* btreeState,
                               DiskLoc &thisLoc,
                               int &keyOfs,
                               const BSONObj &keyBegin,
                               int keyBeginLen,
                               bool afterVersion,
                               const vector<const BSONElement*>& keyEnd,
                               const vector<bool>& keyEndInclusive,
                               int direction) const {
            return btreeState->getBucket<Version>(thisLoc)->advanceTo(btreeState,
                                                                      thisLoc,
                                                                      keyOfs,
                                                                      keyBegin,
                                                                      keyBeginLen,
                                                                      afterVersion,
                                                                      keyEnd,
                                                                      keyEndInclusive,
                                                                      direction);
        }


        virtual bool keyIsUsed(DiskLoc bucket, int keyOffset) const {
            return bucket.btree<Version>()->k(keyOffset).isUsed();
        }

        virtual BSONObj keyAt(DiskLoc bucket, int keyOffset) const {
            verify(!bucket.isNull());
            const BtreeBucket<Version> *b = bucket.btree<Version>();
            int n = b->getN();
            if (n == b->INVALID_N_SENTINEL) {
                throw UserException(deletedBucketCode, "keyAt bucket deleted");
            }
            dassert( n >= 0 && n < 10000 );
            return keyOffset >= n ? BSONObj() : b->keyNode(keyOffset).key.toBson();
        }

        virtual DiskLoc recordAt(DiskLoc bucket, int keyOffset) const {
            const BtreeBucket<Version> *b = bucket.btree<Version>();
            return b->keyNode(keyOffset).recordLoc;
        }

        virtual void keyAndRecordAt(DiskLoc bucket, int keyOffset, BSONObj* keyOut,
                                    DiskLoc* recordOut) const {
            verify(!bucket.isNull());
            const BtreeBucket<Version> *b = bucket.btree<Version>();

            int n = b->getN();

            // If n is 0xffff the bucket was deleted.
            if (keyOffset < 0 || keyOffset >= n || n == 0xffff || !b->isUsed(keyOffset)) {
                return;
            }

            if (keyOffset >= n) {
                *keyOut = BSONObj();
                *recordOut = DiskLoc();
            } else {
                *keyOut = b->keyNode(keyOffset).key.toBson();
                *recordOut = b->keyNode(keyOffset).recordLoc;
            }
        }

        virtual string dupKeyError(const BtreeInMemoryState* btreeState,
                                   DiskLoc bucket,
                                   const BSONObj& keyObj) const {
            typename Version::KeyOwned key(keyObj);
            return btreeState->getBucket<Version>( bucket )->dupKeyError(btreeState->descriptor(),
                                                                         key);
        }

        virtual DiskLoc advance(const BtreeInMemoryState* btreeState,
                                const DiskLoc& thisLoc,
                                int& keyOfs,
                                int direction,
                                const char* caller) const {
            return btreeState->getBucket<Version>(thisLoc)->advance(thisLoc, keyOfs, direction, caller);
        }
        
        virtual bool chronosNextObj(ID id, 
                                    uint64_t &scanStableMask,
                                    uint64_t &scanMask,
                                    st_mask_map* statusMaskMap,
                                    scan_set* scanSet) const {
            
            uint64_t statusMask = 0x0;
            bool success = false;
            bool read_document = true;
            
            while ( !success )
            {
                //log() << "Size Status Mask: " << statusMaskMap->size() << endl;
                statusMask = statusMaskMap->at(id);
                
                // if OR(Status) is 0, read document
                if ( ( ( scanStableMask ^ statusMask ) & scanMask ) == 0x0 )
                {   
                    // update status mask
                    success = ( (statusMaskMap->at(id)).compare_and_swap( ~( ( ~statusMask ) ^ scanMask ), statusMask ) == statusMask ) ? true : false;
                    read_document = true;
                }
                else
                {
                    success = true;
                    read_document = false;
                    
                    scanSet->at(id) = 1;
                }
            }
            
            return read_document;
            
        }
        
        virtual DiskLoc chronosAdvance(const BtreeInMemoryState* btreeState,
                                       const DiskLoc& thisLoc,
                                       DiskLoc& chronosLoc,
                                       DiskLoc& nextLoc,
                                       st_mask_map_vector* statusMaskMapVector,
                                       ofs_map* ofsMap,
                                       scan_set_vector* scanSetVector,
                                       uint64_t &scanStableMask,
                                       uint64_t &scanMask,
                                       IndexCatalog* indexCatalog,
                                       int idxNumber,
                                       unsigned int* entriesAltered,
                                       int& keyOfs,
                                       int direction,
                                       const char* caller) const {
            DiskLoc loc = btreeState->getBucket<Version>(thisLoc)->advance(thisLoc, keyOfs, direction, caller);
            
            if ( loc.isNull() ) {
                // end of scan
                chronosLoc = DiskLoc();
                return *(new DiskLoc(-4, 0)); // start Chronos execution
            }
            else {
                //log() << "ID = " << this->keyAt(loc, keyOfs).getField("").toInt() << endl;
                //log() << "a: " << thisLoc.a() << " | ofs: " << thisLoc.getOfs() << endl;
                
                nextLoc = this->recordAt(loc, keyOfs);
                int _a = nextLoc.a();
                ID _id = nextLoc.id(ofsMap->at(_a));
                //log() << endl << "a: " << _a << " | id: " << _id << endl;
                IDX_MASK_STATUS idxMaskStatus = indexCatalog->checkIndexStatus(_a, _id, idxNumber, scanMask);
                
                if ( idxMaskStatus == UNALTERED )
                {
                    //log() << "UNALTERED" << endl;
                    if ( !chronosNextObj( _id, scanStableMask, scanMask, &(statusMaskMapVector->at(_a)), &(scanSetVector->at(_a)) ) ) {
                        //log() << "Ops, we should not read this record" << endl;
                        chronosLoc = *(new DiskLoc(-3, 0)); //invalid
                    }
                    else {
                        //log() << "Reading this record" << endl;
                        chronosLoc = DiskLoc();
                    }
                }
                else if ( idxMaskStatus == REMOVED )
                {
                    //log() << "REMOVED" << endl;
                    ( &(scanSetVector->at(_a)) )->at(_id) = 1;
                    (*entriesAltered)++;
                    chronosLoc = *(new DiskLoc(-3, 0)); //invalid
                }
                else if ( idxMaskStatus == INSERTED )
                {
                    //log() << "INSERTED" << endl;
                    (*entriesAltered)++;
                    chronosLoc = *(new DiskLoc(-3, 0)); //invalid
                }
                else
                {
                    //log() << "SKIP" << endl;
                    chronosLoc = *(new DiskLoc(-3, 0)); //invalid
                }
            }
            
            return loc;
            
        }
        
        virtual bool chronosVerify(const BtreeInMemoryState* btreeState,
                                   const DiskLoc& thisLoc,
                                   DiskLoc& nextLoc,
                                   st_mask_map_vector* statusMaskMapVector,
                                   ofs_map* ofsMap,
                                   scan_set_vector* scanSetVector,
                                   uint64_t &scanStableMask,
                                   uint64_t &scanMask,
                                   IndexCatalog* indexCatalog,
                                   int idxNumber,
                                   unsigned int* entriesAltered,
                                   int& keyOfs) const {
            nextLoc = this->recordAt(thisLoc, keyOfs);
            int _a = nextLoc.a();
            ID _id = nextLoc.id(ofsMap->at(_a));
            IDX_MASK_STATUS idxMaskStatus = indexCatalog->checkIndexStatus(_a, _id, idxNumber, scanMask); 
            
            if ( idxMaskStatus == REMOVED )
            {
                (*entriesAltered)++;
                ( &(scanSetVector->at(_a)) )->at(_id) = 1;
                return false;
            }
            else if ( idxMaskStatus == INSERTED )
            {
                (*entriesAltered)++;
                return false;
            }
            else if ( idxMaskStatus == UNALTERED )
                return chronosNextObj( _id, scanStableMask, scanMask, &(statusMaskMapVector->at(_a)), &(scanSetVector->at(_a)) );
            else // SKIP
                return false;
        }

        virtual long long fullValidate(const BtreeInMemoryState* btreeState,
                                       const DiskLoc& thisLoc,
                                       const BSONObj& keyPattern) {
            return btreeState->getBucket<Version>(thisLoc)->fullValidate(thisLoc, keyPattern);
        }
    };

    BtreeInterfaceImpl<V0> interface_v0;
    BtreeInterfaceImpl<V1> interface_v1;
    BtreeInterface* BtreeInterface::interfaces[] = { &interface_v0, &interface_v1 };

}  // namespace mongo

