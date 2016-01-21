// collection.cpp

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

//#include <type_traits>
//#include <map>

#include "mongo/db/structure/collection.h"

#include "mongo/base/counter.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/commands/server_status.h"
#include "mongo/db/curop.h"
#include "mongo/db/database.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/namespace_details.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/storage/extent.h"
#include "mongo/db/storage/extent_manager.h"
#include "mongo/db/structure/collection_iterator.h"

#include "mongo/db/pdfile.h" // XXX-ERH
#include "mongo/db/auth/user_document_parser.h" // XXX-ANDY

namespace mongo {

    Collection::Collection( const StringData& fullNS,
                            NamespaceDetails* details,
                            Database* database )
        : _ns( fullNS ),
          _recordStore( _ns.ns() ),
          _infoCache( this ),
          _indexCatalog( this, details ) {
        _details = details;
        _database = database;
        _recordStore.init( _details,
                           &database->getExtentManager(),
                           _ns.coll() == "system.indexes" );
        _magic = 1357924;
        
        stableMask = 0x0; // vector of 0's
        localActiveMask = 0xFFFFFFFFFFFFFFFF; // vector of 1's
        localIndexActiveMask = 0xFFFFFFFFFFFFFFFF; // vector of 1's
        statusMaskMapInit = false;
        documentSetKey = 0;
        documentSetSize = 0;
        indexScanId = 0;
        
        //worstQueueSize = 0;
        //noReclamationQueueSize = 0;
        
        log() << "Init for collection " << _ns.ns() << endl;
        /*log() << "NumRecords:" << numRecords() << endl;*/
        
        //isUserCollection = ( _ns.coll().find( "system." ) == string::npos ) && ( _ns.db() != "local" );
        //isUserCollection = ( _ns.db() == "testdb_100m" );
        isUserCollection = ( _ns.db() == "testdb_1m" ) || ( _ns.db() == "testdb_10m" );
        //isUserCollection = ( _ns.coll().find("test") != string::npos );
        //isUserCollection = false;
        
        statusMaskMapVector.reserve(20);
        //indexScanMap = boost::unordered_map< int, index_scan_info >();
        
        documentSet = doc_set( 6000000 );
    }

    Collection::~Collection() {
        // garbage collection of in-memory structures
        statusMaskMapVector.clear();
        documentSet.clear();
        
        log() << "Exit for collection " << _ns.ns() << endl;
        
        verify( ok() );
        _magic = 0;
    }
    
    Database* Collection::getDatabase()
    {
        return _database;
    }
    
    ID Collection::insertValue(const DiskLoc& loc, uint64_t mask)
    {
        int a = loc.a();
        int ofs = loc.getOfs();
        
        tbb::atomic<uint64_t> init_atomic_mask;
        init_atomic_mask = 0x0;
        
        if ( a == (int)statusMaskMapVector.size() )
        {
            statusMaskMapVector.push_back( st_mask_map( ) );
            ofsMap.push_back(ofs);
            max_id.push_back(0);
            idOfsMap.push_back( ofs_map( ) );
            
            statusMaskMapVector[a].reserve( 800000 );
            idOfsMap[a].reserve( 800000 );
        }
        else if ( a > (int)statusMaskMapVector.size() )
        {
            while ( a >= (int)statusMaskMapVector.size() )
            {
                statusMaskMapVector.push_back( st_mask_map( ) );
                ofsMap.push_back(ofs);
                max_id.push_back(0);
                idOfsMap.push_back( ofs_map( ) );
            }
            statusMaskMapVector[a].reserve( 800000 );
            idOfsMap[a].reserve( 800000 );
        }
        
        ID id = loc.id(ofsMap[a]);
        tbb::atomic<uint64_t> atomic_mask;
        atomic_mask = mask;
        st_mask_map* _statusMaskMap = &statusMaskMapVector[a];
        
        while (id >= _statusMaskMap->size()) {
            _statusMaskMap->push_back( init_atomic_mask );
            idOfsMap[a].push_back(0);
        }
        
        //log() << "Size: " << _statusMaskMap->size() << endl;
        
        _statusMaskMap->at(id) = atomic_mask;
        idOfsMap[a][id] = ofs;
        
        if (id > max_id[a])
            max_id[a] = id;
        
        return id;
    }
    
    void Collection::initializeStatusMaskMap() {
        
        /* Initializing the status mask of each document in the collection. */
        if (isUserCollection) {
            
            //statusMaskMapWriteLock w_lock(SMMLock);
            
            if (!statusMaskMapInit) {
        
                CollectionScanParams params;
                params.ns = _ns.ns();
                params.direction = CollectionScanParams::FORWARD;
                params.tailable = false;
                params.start = DiskLoc();
        
                // Create an executor to handle the scan.
                WorkingSet* ws = new WorkingSet();
                PlanStage* ps = new CollectionScan(params, ws, NULL);
                PlanExecutor runner(ws, ps);
        
                // TODO: catch should be more specific
                try {
                    DiskLoc obj;
                    while (Runner::RUNNER_ADVANCED == runner.getNext(NULL, &obj)) {
                        //log() << obj.getOfs() << endl;
                        insertValue(obj, computeStatusMask());
                    }
                    
                    // allocating more memory for status mask
                    for (int a = 0; a < (int)statusMaskMapVector.size(); a++)
                    {
                        statusMaskMapVector[a].reserve( statusMaskMapVector[a].size()*3 );
                        idOfsMap[a].reserve( idOfsMap[a].size()*3 );
                    }
                    
                    log() << "Status Mask Map initialized for " << _ns.ns() << endl;
                    
                    _indexCatalog.initializeIndexMaskMap();
                    
                    statusMaskMapInit = true;
                } catch (...) {
                    statusMaskMapVector.clear();
                    log() << "Status Mask Map not initialized for " << _ns.ns() << endl;
                    statusMaskMapInit = false;
                }

            }
            
            log() << "StatusMaskMap Size: " << statusMaskMapVector.size() << endl;
            printStatusMaskMap();
            
        }

    }
    
    void Collection::printStatusMaskMap() {
            
        log() << "Printing Status Mask Map..." << endl;
        
        for (int i = 0; i < (int)statusMaskMapVector.size(); i++)
        {
            st_mask_map* statusMaskMap = &statusMaskMapVector[i];
            st_mask_map::iterator it;
            
            log() << "Size: " << statusMaskMap->size() << endl;
            
            for( it = statusMaskMap->begin(); it != statusMaskMap->end(); it++) {
                
                /*log() << "Value: " << std::hex << *it << endl;*/
            }
        }
    }
    
    void Collection::printDocumentSet() {
            
        log() << "Printing Document Set..." << endl;
        
        doc_set::iterator it;
        uint64_t key;
        queue_document value;
        
        for( it = documentSet.begin(); it != documentSet.end(); it++) {
            key = it->first;
            value = it->second;
            
            log() << "Key: " << key << " | Value: " << value.document.toString() << " & " << value.queueStatusMask << endl;
        }
    }
    
    uint64_t Collection::computeStatusMask() {
        return ~ ( stableMask ^ localActiveMask );
    }
                
    uint64_t Collection::computeQueueStatusMask(uint64_t statusMask) {
        return ( (stableMask ^ statusMask) | localActiveMask );
    }

    bool Collection::requiresIdIndex() const {

        if ( _ns.ns().find( '$' ) != string::npos ) {
            // no indexes on indexes
            return false;
        }

        if ( _ns == _database->_namespacesName ||
             _ns == _database->_indexesName ||
             _ns == _database->_extentFreelistName ||
             _ns == _database->_profileName ) {
            return false;
        }

        if ( _ns.db() == "local" ) {
            if ( _ns.coll().startsWith( "oplog." ) )
                return false;
        }

        if ( !_ns.isSystem() ) {
            // non system collections definitely have an _id index
            return true;
        }


        return true;
    }

    CollectionIterator* Collection::getIterator( const DiskLoc& start, bool tailable,
                                                 const CollectionScanParams::Direction& dir,
                                                 bool use_chronos,
                                                 uint64_t scanMask,
                                                 uint64_t scanStableMask) {
        
        verify( ok() );
        if ( _details->isCapped() )
            return new CappedIterator( this, start, tailable, dir, use_chronos, scanMask, scanStableMask );
        return new FlatIterator( this, start, dir, use_chronos, scanMask, scanStableMask );
    }

    BSONObj Collection::docFor( const DiskLoc& loc ) {
        Record* rec = getExtentManager()->recordFor( loc );
        return BSONObj::make( rec->accessed() );
    }

    StatusWith<DiskLoc> Collection::insertDocument( const DocWriter* doc, bool enforceQuota ) {
        verify( _indexCatalog.numIndexesTotal() == 0 ); // eventually can implement, just not done
        
        // TODO: how to insert status mask here ?

        StatusWith<DiskLoc> loc = _recordStore.insertRecord( doc,
                                                             enforceQuota ? largestFileNumberInQuota() : 0 );
        if ( !loc.isOK() )
            return loc;

        return StatusWith<DiskLoc>( loc );
    }

    StatusWith<DiskLoc> Collection::insertDocument( const BSONObj& docToInsert, bool enforceQuota ) {
        
        if ( _indexCatalog.findIdIndex() ) {
            if ( docToInsert["_id"].eoo() ) {
                return StatusWith<DiskLoc>( ErrorCodes::InternalError,
                                            "Collection::insertDocument got document without _id" );
            }
        }

        if ( _details->isCapped() ) {
            // TOOD: old god not done
            Status ret = _indexCatalog.checkNoIndexConflicts( docToInsert );
            if ( !ret.isOK() )
                return StatusWith<DiskLoc>( ret );
        }
        
        StatusWith<DiskLoc> status = StatusWith<DiskLoc>( ErrorCodes::InternalError,
                "Initial Status" );
        
        status = _insertDocument( docToInsert, enforceQuota );
        if ( status.isOK() )
            _details->paddingFits();

        return status;
    }

    StatusWith<DiskLoc> Collection::_insertDocument( const BSONObj& docToInsert, bool enforceQuota ) {

        // TODO: for now, capped logic lives inside NamespaceDetails, which is hidden
        //       under the RecordStore, this feels broken since that should be a
        //       collection access method probably

        StatusWith<DiskLoc> loc = _recordStore.insertRecord( docToInsert.objdata(),
                                                             docToInsert.objsize(),
                                                            enforceQuota ? largestFileNumberInQuota() : 0 );
        if ( !loc.isOK() )
            return loc;

        _infoCache.notifyOfWriteOp();
        
        // ---- VLS ---- //
                        
        // inserting status mask
        if (isUserCollection)
            insertValue(loc.getValue(), computeStatusMask());
        
        // ---- VLS ---- //

        try {
            _indexCatalog.indexRecord( docToInsert, loc.getValue() );
        }
        catch ( AssertionException& e ) {
            if ( _details->isCapped() ) {
                return StatusWith<DiskLoc>( ErrorCodes::InternalError,
                                            str::stream() << "unexpected index insertion failure on"
                                            << " capped collection" << e.toString()
                                            << " - collection and its index will not match" );
            }

            // indexRecord takes care of rolling back indexes
            // so we just have to delete the main storage
            _recordStore.deleteRecord( loc.getValue() );
            return StatusWith<DiskLoc>( e.toStatus( "insertDocument" ) );
        }

        return loc;
    }

    void Collection::deleteDocument( const DiskLoc& loc, bool cappedOK, bool noWarn,
                                     BSONObj* deletedId ) {
        
        if ( _details->isCapped() && !cappedOK ) {
            log() << "failing remove on a capped ns " << _ns << endl;
            uasserted( 10089,  "cannot remove from a capped collection" );
            return;
        }

        BSONObj doc = docFor( loc );

        if ( deletedId ) {
            BSONElement e = doc["_id"];
            if ( e.type() ) {
                *deletedId = e.wrap();
            }
        }

        /* check if any cursors point to us.  if so, advance them. */
        ClientCursor::aboutToDelete(_ns.ns(), _details, loc);
        
        // ---- VLS ---- //
        
        if (isUserCollection) {
            
            // before deleting the document, check if we need to copy it to
            // the shared queue (documentSet)
        	int _a = loc.a();
        	ID id = loc.id(ofsMap[_a]);
        	uint64_t queueStatusMask;
            
        	st_mask_map* _statusMaskMap = &statusMaskMapVector[_a];
                    
			// computing Queue Status Mask
			queueStatusMask = computeQueueStatusMask( _statusMaskMap->at(id) );
                    
			// if AND(Queue Status Mask) is 0, document must be copied to queue first
            if ( queueStatusMask != 0xFFFFFFFFFFFFFFFF ) {
                
                // adding document to shared document set
                documentSet.insert( std::make_pair( documentSetKey++, queue_document(doc, id, _a, queueStatusMask) ) );
                documentSetSize += 1;
            }
            
            // removing status mask from the map
            _statusMaskMap->at(id) = 0x0;
        }
        
        // ---- VLS ---- //
        
        _indexCatalog.unindexRecord( doc, loc, noWarn);

        _recordStore.deleteRecord( loc );

        _infoCache.notifyOfWriteOp();
    }

    Counter64 moveCounter;
    ServerStatusMetricField<Counter64> moveCounterDisplay( "record.moves", &moveCounter );

    StatusWith<DiskLoc> Collection::updateDocument( const DiskLoc& oldLocation,
                                                    const BSONObj& objNew,
                                                    bool enforceQuota,
                                                    OpDebug* debug ) {

        Record* oldRecord = getExtentManager()->recordFor( oldLocation );
        BSONObj objOld = BSONObj::make( oldRecord );

        if ( objOld.hasElement( "_id" ) ) {
            BSONElement oldId = objOld["_id"];
            BSONElement newId = objNew["_id"];
            if ( oldId != newId )
                return StatusWith<DiskLoc>( ErrorCodes::InternalError,
                                            "in Collection::updateDocument _id mismatch",
                                            13596 );
        }

        if ( ns().coll() == "system.users" ) {
            // XXX - andy and spencer think this should go away now
            V2UserDocumentParser parser;
            Status s = parser.checkValidUserDocument(objNew);
            if ( !s.isOK() )
                return StatusWith<DiskLoc>( s );
        }

        /* duplicate key check. we descend the btree twice - once for this check, and once for the actual inserts, further
           below.  that is suboptimal, but it's pretty complicated to do it the other way without rollbacks...
        */
        OwnedPointerVector<UpdateTicket> updateTickets;
        updateTickets.mutableVector().resize(_indexCatalog.numIndexesTotal());
        for (int i = 0; i < _indexCatalog.numIndexesTotal(); ++i) {
            IndexDescriptor* descriptor = _indexCatalog.getDescriptor( i );
            IndexAccessMethod* iam = _indexCatalog.getIndex( descriptor );

            InsertDeleteOptions options;
            options.logIfError = false;
            options.dupsAllowed =
                !(KeyPattern::isIdKeyPattern(descriptor->keyPattern()) || descriptor->unique())
                || ignoreUniqueIndex(descriptor);
            updateTickets.mutableVector()[i] = new UpdateTicket();
            Status ret = iam->validateUpdate(objOld, objNew, oldLocation, options,
                                             updateTickets.mutableVector()[i]);
            if ( !ret.isOK() ) {
                return StatusWith<DiskLoc>( ret );
            }
        }

        if ( oldRecord->netLength() < objNew.objsize() ) {
            // doesn't fit, have to move to new location

            if ( _details->isCapped() )
                return StatusWith<DiskLoc>( ErrorCodes::InternalError,
                                            "failing update: objects in a capped ns cannot grow",
                                            10003 );

            moveCounter.increment();
            _details->paddingTooSmall();

            // unindex old record, don't delete
            // this way, if inserting new doc fails, we can re-index this one
            ClientCursor::aboutToDelete(_ns.ns(), _details, oldLocation);
            _indexCatalog.unindexRecord( objOld, oldLocation, true );

            if ( debug ) {
                if (debug->nmoved == -1) // default of -1 rather than 0
                    debug->nmoved = 1;
                else
                    debug->nmoved += 1;
            }

            StatusWith<DiskLoc> loc = _insertDocument( objNew, enforceQuota );
            
            ID id;
            int _a;

            if ( loc.isOK() ) {
                // insert successful, now lets deallocate the old location
                // remember its already unindexed
                _recordStore.deleteRecord( oldLocation );
                
                // removing oldLocation from map
                ID old_id = oldLocation.id(ofsMap[oldLocation.a()]);
                st_mask_map* _statusMaskMap = &statusMaskMapVector[oldLocation.a()];
                uint64_t statusMask = _statusMaskMap->at(old_id);
                _statusMaskMap->at(old_id) = 0x0;
                idOfsMap[oldLocation.a()][old_id] = 0;
                
                // adding new location to map
                _a = loc.getValue().a();
                id = insertValue(loc.getValue(), statusMask);
            }
            else {
                // new doc insert failed, so lets re-index the old document and location
                _indexCatalog.indexRecord( objOld, oldLocation );
                
                _a = oldLocation.a();
                id = oldLocation.id(ofsMap[_a]);
            }
            
            // ---- VLS ---- //
                            
            if (isUserCollection) {
                        
                // before updating the document, check if we need to copy it to
                // the shared queue (documentSet)
                //st_mask_map* _statusMaskMap = &statusMaskMapVector[_a];
                
                uint64_t queueStatusMask;
                        
                // computing Queue Status Mask
                queueStatusMask = computeQueueStatusMask( (&statusMaskMapVector[_a])->at(id) );
                
                //log() << "[Update] Status mask from document a=" << id.first << ", ofs=" << id.second << " is " << statusMask << endl;
                
                // if AND(Queue Status Mask) is 0, document must be copied to queue first
                if ( queueStatusMask != 0xFFFFFFFFFFFFFFFF ) {
                    
                    // updating status mask in the map
                    (&statusMaskMapVector[_a])->at(id) = computeStatusMask();
                    
                    // adding document to shared document set
                    documentSet.insert( std::make_pair( documentSetKey++, queue_document(objOld, id, _a, queueStatusMask) ) );
                    documentSetSize += 1;
                }
            }
            
            // ---- VLS ---- //

            return loc;
        }
        
        // ---- VLS ---- //
                
        if (isUserCollection) {
                    
            // before updating the document, check if we need to copy it to
            // the shared queue (documentSet)
            int _a = oldLocation.a();
            ID id = oldLocation.id(ofsMap[_a]);
            uint64_t queueStatusMask;
                    
            // computing Queue Status Mask
            queueStatusMask = computeQueueStatusMask( (&statusMaskMapVector[_a])->at(id) );
            
            //log() << "[Update] Status mask from document a=" << id.first << ", ofs=" << id.second << " is " << statusMask << endl;
            
            // if AND(Queue Status Mask) is 0, document must be copied to queue first
            if ( queueStatusMask != 0xFFFFFFFFFFFFFFFF ) {
                
                // experimental purpose
                //uint64_t statusMask = (&statusMaskMapVector[_a])->at(id); 
                
                // updating status mask in the map
                (&statusMaskMapVector[_a])->at(id) = computeStatusMask();
                
                // experimental purpose
                /*uint64_t debugMask = statusMask ^ computeStatusMask();
                for (int i = 0; i < 64; i++)
                {
                    worstQueueSize += (int) (debugMask & 1);
                    debugMask >>= 1;
                }
                noReclamationQueueSize += 1;*/
                
                // adding document to shared document set
                documentSet.insert( std::make_pair( documentSetKey++, queue_document(objOld, id, _a, queueStatusMask) ) );
                documentSetSize += 1;
                
                // experimental purpose
                /*struct timeval tp;
                gettimeofday(&tp, NULL);
                debug_str << "Queue Size: " << documentSet.size() << "/" << worstQueueSize.fetch_and_add(0) << "/" << noReclamationQueueSize.fetch_and_add(0) << "/" << (tp.tv_sec * 1000000000 + tp.tv_usec * 1000) << endl;*/
            }
        }
        
        // ---- VLS ---- //

        _infoCache.notifyOfWriteOp();
        _details->paddingFits();

        if ( debug )
            debug->keyUpdates = 0;

        for (int i = 0; i < _indexCatalog.numIndexesTotal(); ++i) {
            IndexDescriptor* descriptor = _indexCatalog.getDescriptor( i );
            IndexAccessMethod* iam = _indexCatalog.getIndex( descriptor );

            int64_t updatedKeys;
            iam->setCollection(this);
            Status ret = iam->update(*updateTickets.vector()[i], &updatedKeys);
            if ( !ret.isOK() )
                return StatusWith<DiskLoc>( ret );
            if ( debug )
                debug->keyUpdates += updatedKeys;
        }

        //  update in place
        int sz = objNew.objsize();
        memcpy(getDur().writingPtr(oldRecord->data(), sz), objNew.objdata(), sz);
        return StatusWith<DiskLoc>( oldLocation );
    }

    int64_t Collection::storageSize( int* numExtents, BSONArrayBuilder* extentInfo ) const {
        if ( _details->firstExtent().isNull() ) {
            if ( numExtents )
                *numExtents = 0;
            return 0;
        }

        Extent* e = getExtentManager()->getExtent( _details->firstExtent() );

        long long total = 0;
        int n = 0;
        while ( e ) {
            total += e->length;
            n++;

            if ( extentInfo ) {
                extentInfo->append( BSON( "len" << e->length << "loc: " << e->myLoc.toBSONObj() ) );
            }

            e = getExtentManager()->getNextExtent( e );
        }

        if ( numExtents )
            *numExtents = n;

        return total;
    }

    ExtentManager* Collection::getExtentManager() {
        verify( ok() );
        return &_database->getExtentManager();
    }

    const ExtentManager* Collection::getExtentManager() const {
        verify( ok() );
        return &_database->getExtentManager();
    }

    Extent* Collection::increaseStorageSize( int size, bool enforceQuota ) {
        return getExtentManager()->increaseStorageSize( _ns,
                                                        _details,
                                                        size,
                                                        enforceQuota ? largestFileNumberInQuota() : 0 );
    }

    int Collection::largestFileNumberInQuota() const {
        if ( !storageGlobalParams.quota )
            return 0;

        if ( _ns.db() == "local" )
            return 0;

        if ( _ns.isSpecial() )
            return 0;

        return storageGlobalParams.quotaFiles;
    }

    bool Collection::isCapped() const {
        return _details->isCapped();
    }

    uint64_t Collection::numRecords() const {
        return _details->numRecords();
    }

    uint64_t Collection::dataSize() const {
        return _details->dataSize();
    }
    
    // Fillping bits as a background job
    
    FlipPhase::FlipPhase() {}
    
    FlipPhase::FlipPhase(Database* db, Collection* collection,
            uint64_t scanStableMask, uint64_t scanMask)
    : _db(db),
      _collection(collection),
      _scanStableMask(scanStableMask),
      _scanMask(scanMask) {}
    
    FlipPhase::~FlipPhase() {}
    
    void FlipPhase::run()
    {
        Client::initThread( "flipping-bits" );
        
        bool stableIsZero = (_scanStableMask == 0x0) ? true : false;
        
        for (int i = 0; i < (int)_collection->statusMaskMapVector.size(); i++)
        {
            st_mask_map* statusMaskMap = &(_collection->statusMaskMapVector[i]);
            for( st_mask_map::iterator it = statusMaskMap->begin();
                    it != statusMaskMap->end(); it++)
            {
                Lock::DBRead lk( _db->name() );
                
                if ( stableIsZero )
                    (it)->fetch_and_store( _scanMask | (*it) );
                else
                    (it)->fetch_and_store( ~_scanMask & (*it) );
                
            }
        }
        
        // updating Active Mask and Stable Mask
        // note that Stable Mask can be updated before scanning the queue
        {
            activeMaskWriteLock amw_lock(_db->AMLock);
                          
            _db->activeMask |= _scanMask;
            _db->indexActiveMask |= _scanMask;
            
            //log() << "Active Mask (2): " << std::hex << _db->activeMask << endl;
            //log() << "Stable Mask (2): " << std::hex << _collection->stableMask << endl;
            
            // allow an awaiting scan to start
            _db->activeMaskFull = false;   
            
        } // releasing active mask lock
        
        _db->activeMaskCondition.notify_one();

    }

}
