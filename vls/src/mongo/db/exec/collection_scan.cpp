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

#include "mongo/db/exec/collection_scan.h"

#include "mongo/db/exec/collection_scan_common.h"
#include "mongo/db/exec/filter.h"
#include "mongo/db/exec/working_set.h"
#include "mongo/db/structure/collection_iterator.h"

#include "mongo/db/client.h" // XXX-ERH
#include "mongo/db/pdfile.h" // XXX-ERH/ACM

namespace mongo {

    CollectionScan::CollectionScan(const CollectionScanParams& params,
                                   WorkingSet* workingSet,
                                   const MatchExpression* filter,
                                   bool use_chronos)
        : _workingSet(workingSet),
          _filter(filter),
          _params(params),
          _nsDropped(false),
          _use_chronos(use_chronos),
          scanMask( 0x0 ),
          documentSetKeysInit( false ),
          sharedQueueScanDone( false ) {
        
        // --------- VLS --------- //
        
        documentSetKeysIndex = -1;
        documentSetKeysSize = 0;
        
        // getting database and collection
        database = cc().database();
        collection = database->getCollection( _params.ns );
        
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
                        collection->localActiveMask &= ~(mask << n);
                        scanMask = (mask << n);
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
                
            } else {
                _use_chronos = false;
            }
        } else {
            _use_chronos = false;
        }
        
        //log() << "Collection Scan Initialized!" << endl;
        
        // --------- VLS --------- //
        
    }
    
    bool CollectionScan::chronosNextObj(DiskLoc &nextLoc, BSONObj &nextObj) {
        
        bool skip = false;
            
        // "materializing" the document set for this particular scan
        // since this scan will not need new documents added to the set
        if ( !documentSetKeysInit ) {
            
            if ( collection->documentSetSize == 0 )
                sharedQueueScanDone = true;
            else
            {
                {
                    // locking document set -- prevent concurrency with erase operation
                    documentSetWriteLock dsw_lock(collection->DSLock);
                    
                    for( doc_set::const_iterator it = collection->documentSet.begin();
                            it != collection->documentSet.end(); ++it )
                        documentSetKeys.push_back(it->first);
                    
                    // experimental purpose
                    /*struct timeval tp;
                    gettimeofday(&tp, NULL);
                    collection->debug_str << "Queue Size: " << collection->documentSet.size() << "/" << collection->worstQueueSize.fetch_and_add(0) << "/" << collection->noReclamationQueueSize.fetch_and_add(0) << "/" << tp.tv_sec * 1000000000 + tp.tv_usec * 1000 << endl;*/
                }
    
                documentSetKeysInit = true;
                documentSetKeysSize = documentSetKeys.size();
            }

        }
        
        // if scan in shared queue is still not over...
        if ( !sharedQueueScanDone ) {
            
            bool nextDocument = true;
            bool find = false;
            queue_document queueDocument;
            uint64_t queueStatusMask = 0x0;
            
            while ( nextDocument ) {
                
                if ( (++documentSetKeysIndex + 1) <= documentSetKeysSize ) {

                    doc_set::accessor a;
                    find = collection->documentSet.find(a, documentSetKeys[documentSetKeysIndex]);

                    // document has not been removed from shared queue
                    if ( find ) {
                        
                        // getting the document from the shared queue
                        queueDocument = a->second;
                        queueStatusMask = queueDocument.queueStatusMask;
                        
                        // if OR(Status) is 0, read the document from the queue
                        // status = ( scan mask & queue status mask )
                        if ( ( scanMask & queueStatusMask ) == 0x0 ) {
                            
                            // read document
                            nextDocument = false;
                            skip = true;
                            
                            nextLoc = *(new DiskLoc(-3, 0)); //invalid
                            nextObj = queueDocument.document;
                            
                            // computing new Queue Status Mask
                            queueStatusMask |= scanMask;
                            
                            // experimental purpose
                            /*{
                                documentSetWriteLock dsw_lock(collection->DSLock);
                                struct timeval tp;
                                gettimeofday(&tp, NULL);
                                collection->debug_str << "Queue Size: " << collection->documentSet.size() << "/" << collection->worstQueueSize.fetch_and_decrement() - 1 << "/" << collection->noReclamationQueueSize.fetch_and_add(0) << "/" << tp.tv_sec * 1000000000 + tp.tv_usec * 1000 << endl;
                            }*/
                            
                            // if AND(New Queue Status Mask) is 1
                            // document can be removed from queue
                            if ( queueStatusMask == 0xFFFFFFFFFFFFFFFF ) {
                                {
                                    // locking document set -- prevent concurrency with traversal
                                    documentSetWriteLock dsw_lock(collection->DSLock);
                                    collection->documentSet.erase(a);
                                    
                                    // experimental purpose
                                    /*struct timeval tp;
                                    gettimeofday(&tp, NULL);
                                    collection->debug_str << "Queue Size: " << collection->documentSet.size() << "/" << collection->worstQueueSize.fetch_and_add(0) << "/" << collection->noReclamationQueueSize.fetch_and_add(0) << "/" << tp.tv_sec * 1000000000 + tp.tv_usec * 1000 << endl;*/
                                }
                                
                                collection->documentSetSize -= 1;
                                
                                //log() << "Removing " << queueDocument.document.getField("_id").toInt() << endl;
                                //log() << "Size of queue: " << collection->documentSet.size() << endl;
                            }
                            
                            // if AND(New Queue Status Mask) is 0
                            // update Queue Status Mask
                            else {
                                queueDocument.queueStatusMask = queueStatusMask;
                                
                                // updating document
                                a->second = queueDocument;
                            }
                            
                        }
                        
                    }
                    //a.release();
                    
                } else {
                    nextDocument = false;
                }
                
            } // while
            
        } // if
        
        if ( !skip )
        {
            // no more documents to be read by the scan
            // free the bits
            if ( (documentSetKeysIndex + 1) >= documentSetKeysSize ) {
                
                // updating Active Mask and Stable Mask
                // note that Stable Mask can be updated before scanning the queue
                {
                    activeMaskWriteLock amw_lock(database->AMLock);
                    
                    {
                        stableMaskWriteLock smw_lock(collection->SMLock);
                        
                        database->activeMask |= scanMask;
                        collection->localActiveMask |= scanMask;
                        collection->stableMask = ~( ( ~collection->stableMask ) ^ scanMask );
                        
                        //log() << "Active Mask: " << std::hex << database->activeMask << endl;
                        //log() << "Stable Mask: " << std::hex << collection->stableMask << endl;
                        
                        // allow an awaiting scan to start
                        database->activeMaskFull = false;
                        
                        // experimental purposes
                        /*if (database->activeMask == 0xFFFFFFFFFFFFFFFF)
                        {
                            std::ofstream out("PATH");
                            out << collection->debug_str.str();
                            out.close();
                        }*/
                        
                    } // releasing stable mask lock 
                    
                } // releasing active mask lock
                
                database->activeMaskCondition.notify_one();
                
            }
        }
        
        return skip;

    }

    PlanStage::StageState CollectionScan::work(WorkingSetID* out) {
        ++_commonStats.works;
        if (_nsDropped) { return PlanStage::DEAD; }

        if (NULL == _iter) {
            collection = cc().database()->getCollection( _params.ns );
            if ( collection == NULL ) {
                _nsDropped = true;
                return PlanStage::DEAD;
            }

            _iter.reset( collection->getIterator( _params.start,
                                                  _params.tailable,
                                                  _params.direction,
                                                  _use_chronos,
                                                  scanMask,
                                                  scanStableMask) );

            ++_commonStats.needTime;
            return PlanStage::NEED_TIME;
        }

        DiskLoc nextLoc;
        BSONObj nextObj;

        // Should we try getNext() on the underlying _iter if we're EOF?  Yes, if we're tailable.
        if ( isEOF() ) {
            
            bool skip = false;
            if ( _use_chronos )
                skip = chronosNextObj(nextLoc, nextObj);
            
            if ( !skip )
            {
                if (!_params.tailable) {
                    return PlanStage::IS_EOF;
                }
                else {
                    // See if _iter gives us anything new.
                    nextLoc = _iter->getNext(nextObj);
                    if (nextLoc.isNull()) {
                        // Nope, still EOF.
                        return PlanStage::IS_EOF;
                    }
                }
            }
        }
        else {
            nextLoc = _iter->getNext(nextObj);
            // go to document set if nextLoc is null
            if (nextLoc.isNull())
                chronosNextObj(nextLoc, nextObj);
            else if (nextLoc.isChronosInvalid())
            {
                ++_commonStats.needTime;
                return PlanStage::NEED_TIME;
            }
        }
        
        WorkingSetID id = _workingSet->allocate();
        WorkingSetMember* member = _workingSet->get(id);
        member->loc = nextLoc;
        member->obj = nextObj;
        member->state = WorkingSetMember::LOC_AND_UNOWNED_OBJ;

        ++_specificStats.docsTested;

        if (Filter::passes(member, _filter)) {
            *out = id;
            ++_commonStats.advanced;
            return PlanStage::ADVANCED;
        }
        else {
            _workingSet->free(id);
            ++_commonStats.needTime;
            return PlanStage::NEED_TIME;
        }
    }

    bool CollectionScan::isEOF() {
        if ((0 != _params.maxScan) && (_specificStats.docsTested >= _params.maxScan)) {
            return true;
        }
        if (_nsDropped) { return true; }
        if (NULL == _iter) { return false; }
        return _iter->isEOF();
    }

    void CollectionScan::invalidate(const DiskLoc& dl) {
        ++_commonStats.invalidates;
        if (NULL != _iter) {
            _iter->invalidate(dl);
        }
    }

    void CollectionScan::prepareToYield() {
        ++_commonStats.yields;
        if (NULL != _iter) {
            _iter->prepareToYield();
        }
    }

    void CollectionScan::recoverFromYield() {
        ++_commonStats.unyields;
        if (NULL != _iter) {
            if (!_iter->recoverFromYield()) {
                warning() << "collection dropped during yield of collscan or state deleted";
                _nsDropped = true;
            }
        }
    }

    PlanStageStats* CollectionScan::getStats() {
        _commonStats.isEOF = isEOF();
        auto_ptr<PlanStageStats> ret(new PlanStageStats(_commonStats, STAGE_COLLSCAN));
        ret->specific.reset(new CollectionScanStats(_specificStats));
        return ret.release();
    }

}  // namespace mongo
