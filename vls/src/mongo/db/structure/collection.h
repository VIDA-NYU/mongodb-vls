// collection.h

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
*    Copyright (C) 2012 10gen Inc.
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

#pragma once

#include <string>
#include <sstream>

#include <boost/thread/locks.hpp>
#include <boost/thread.hpp>
#include <boost/unordered_map.hpp>

#include "mongo/base/string_data.h"
#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/diskloc.h"
#include "mongo/db/exec/collection_scan_common.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/structure/record_store.h"
#include "mongo/db/structure/collection_info_cache.h"
#include "mongo/db/query/index_bounds.h"
#include "mongo/platform/cstdint.h"

#include "mongo/db/exec/collection_scan.h"
#include "mongo/db/exec/plan_stage.h"
#include "mongo/db/query/plan_executor.h"

#include "mongo/util/background.h"

#include "tbb/concurrent_hash_map.h"
#include "tbb/atomic.h"

#include <iostream>
#include <utility>
#include <vector>

/* For experimental purposes */
//#include <sys/time.h>
//#include <boost/timer.hpp>
//#include <sstream>
//#include <fstream>

namespace mongo {

    class Database;
    class ExtentManager;
    class NamespaceDetails;
    class IndexCatalog;

    class CollectionIterator;
    class FlatIterator;
    class CappedIterator;

    class OpDebug;

    class DocWriter {
    public:
        virtual ~DocWriter() {}
        virtual void writeDocument( char* buf ) const = 0;
        virtual size_t documentSize() const = 0;
    };
    
    // ---- VLS ---- //
    
    /* Identifies a Disk Loc (_a, ofs)*/
    typedef uint32_t ID;
    
    /**
     * Used to represent a document in the shared queue (document set).
     */
    struct queue_document {
        BSONObj document;
        ID id;
        int _a;
        uint64_t queueStatusMask;
        
        queue_document()
                : queueStatusMask( 0x0 ) { }
        
        queue_document(BSONObj doc, ID id, int _a, uint64_t queueStatusMask)
                : document( doc.copy() ),
                  id( id ),
                  _a( _a ),
                  queueStatusMask( queueStatusMask ) { }
        
        queue_document(const queue_document& queueDocument) {
            document = queueDocument.document;
            id = queueDocument.id;
            _a = queueDocument._a;
            queueStatusMask = queueDocument.queueStatusMask;
        }
    };
    
    /* Scan Set */
    typedef std::vector< uint8_t > scan_set;
    
    /* Vector of Scan Set */
    typedef std::vector<scan_set> scan_set_vector;
    
    /**
     * Used to pass information about each index scan to the collection.
     * For index update purposes.
     */
    struct index_scan_info {
        const IndexBounds bounds;
        const BSONObj keyPattern;
        int direction;
        scan_set_vector* scanSetVector;
        
        index_scan_info() { }
        
        index_scan_info(const IndexBounds _bounds, const BSONObj _keyPattern,
                int _direction, scan_set_vector* _scanSetVector)
                    :  bounds(_bounds),
                       keyPattern(_keyPattern),
                       direction(_direction),
                       scanSetVector(_scanSetVector) { }
        
        /*index_scan_info(const index_scan_info& indexScanInfo) {
            bounds = indexScanInfo.bounds;
            keyPattern = indexScanInfo.keyPattern;
            direction = indexScanInfo.direction;
            scanSetVector = indexScanInfo.scanSetVector;
        }*/
    };
    
    /* Status Mask Map */
    typedef std::vector< tbb::atomic<uint64_t> > st_mask_map;
    
    /* Vector of Status Mask Map -- one per DiskLoc Volume */
    typedef std::vector<st_mask_map> st_mask_map_vector;
    
    /* Document Set */
    typedef tbb::concurrent_hash_map<uint64_t, queue_document> doc_set;
    
    /* Map between DiskLoc Volume and its initial Offset */
    typedef std::vector<int> ofs_map;
    
    /* Stable Mask locks */
    typedef boost::shared_mutex stableMaskLock;
    typedef boost::unique_lock< stableMaskLock > stableMaskWriteLock;
    typedef boost::shared_lock< stableMaskLock > stableMaskReadLock;
    
    /* Document Set locks */
    typedef boost::shared_mutex documentSetLock;
    typedef boost::unique_lock< documentSetLock > documentSetWriteLock;
    
    /* Locks for Resetting Index Status Masks */
    typedef boost::shared_mutex indexMaskLock;
    typedef boost::unique_lock< indexMaskLock > indexMaskWriteLock;
    
    // ---- VLS ---- //

    /**
     * this is NOT safe through a yield right now
     * not sure if it will be, or what yet
     */
    class Collection {
    public:
        Collection( const StringData& fullNS,
                        NamespaceDetails* details,
                        Database* database );

        ~Collection();

        bool ok() const { return _magic == 1357924; }

        NamespaceDetails* details() { return _details; } // TODO: remove
        const NamespaceDetails* details() const { return _details; }

        CollectionInfoCache* infoCache() { return &_infoCache; }
        const CollectionInfoCache* infoCache() const { return &_infoCache; }

        const NamespaceString& ns() const { return _ns; }

        const IndexCatalog* getIndexCatalog() const { return &_indexCatalog; }
        IndexCatalog* getIndexCatalog() { return &_indexCatalog; }
        
        // ---- VLS ---- //
        
        /* Indicates if the collection was created by the user */
        bool isUserCollection;
        
        /* stableMask controls the value of the bit that represents a stable
           version for each vector of bits in the collection */
        uint64_t stableMask;
        
        /* Local active mask */
        uint64_t localActiveMask;
        
        /* Local index active mask */
        uint64_t localIndexActiveMask;
        
        /* Stable Mask lock */
        stableMaskLock SMLock;
        
        /* In-memory set for storing stable version of documents.
           This is what we used to call queue. */
        doc_set documentSet;
        
        /* Key for DocumentSet */
        uint64_t documentSetKey;
        
        /* Size of DocumentSet */
        tbb::atomic<int> documentSetSize;
        
        /* Document Set lock */
        documentSetLock DSLock;
        
        /* Index Mask lock */
        indexMaskLock IMLock;
        
        /* Vector of Status Mask Maps, one per DiskLoc Volume.
           A Status Mask Map is a in-memory map
           that keeps tracks of the Status Mask
           of each document in the collection, based on the _id.
           The key is the _id field (int value), and the value is
           the status mask. */
        st_mask_map_vector statusMaskMapVector;
        
        /* Offset Map */
        ofs_map ofsMap;
        
        /* Stores the maximum id for each DiskLoc */
        std::vector< ID > max_id;
        
        /* ID for Index Scans */
        int indexScanId;
        
        /* Map that contains all active scan sets */
        //boost::unordered_map< int, index_scan_info > indexScanMap;
        
        /* Mapping between ID and ofs value */
        std::vector< ofs_map > idOfsMap;
        
        /* Status Mask Map lock */
        //statusMaskMapLock SMMLock;
        
        /* For experimental purpose */
        //tbb::atomic<int> worstQueueSize;
        //tbb::atomic<int> noReclamationQueueSize;
        //std::stringstream debug_str;
        
        /* Initialize status mask for each document of the collection. */
        void initializeStatusMaskMap();
        
        /* Inserts a mask into status mask map. */
        ID insertValue(const DiskLoc& loc, uint64_t mask);
        
        /* Indicates if the status mask map was initialized. */
        bool statusMaskMapInit;
        
        /* Method used to compute a status mask for a document.
           Warning: This method does not take care of locking! */
        uint64_t computeStatusMask();
            
        /* Method used to compute a queue status mask for a document in the document set.
           Warning: This method does not take care of locking! */
        uint64_t computeQueueStatusMask(uint64_t statusMask);
        
        /* Method to return the database object */
        Database* getDatabase();
        
        /* Prints status mask map */
        void printStatusMaskMap();
        
        /* Prints document set */
        void printDocumentSet();
        
        // ---- VLS ---- //

        bool requiresIdIndex() const;

        BSONObj docFor( const DiskLoc& loc );

        // ---- things that should move to a CollectionAccessMethod like thing

        CollectionIterator* getIterator( const DiskLoc& start, bool tailable,
                                         const CollectionScanParams::Direction& dir,
                                         bool use_chronos = false,
                                         uint64_t scanMask = 0x0,
                                         uint64_t scanStableMask = 0x0);

        void deleteDocument( const DiskLoc& loc,
                             bool cappedOK = false,
                             bool noWarn = false,
                             BSONObj* deletedId = 0 );

        /**
         * this does NOT modify the doc before inserting
         * i.e. will not add an _id field for documents that are missing it
         */
        StatusWith<DiskLoc> insertDocument( const BSONObj& doc, bool enforceQuota );

        StatusWith<DiskLoc> insertDocument( const DocWriter* doc, bool enforceQuota );

        /**
         * updates the document @ oldLocation with newDoc
         * if the document fits in the old space, it is put there
         * if not, it is moved
         * @return the post update location of the doc (may or may not be the same as oldLocation)
         */
        StatusWith<DiskLoc> updateDocument( const DiskLoc& oldLocation,
                                            const BSONObj& newDoc,
                                            bool enforceQuota,
                                            OpDebug* debug );

        int64_t storageSize( int* numExtents = NULL, BSONArrayBuilder* extentInfo = NULL ) const;

        // -----------

        // this is temporary, moving up from DB for now
        // this will add a new extent the collection
        // the new extent will be returned
        // it will have been added to the linked list already
        Extent* increaseStorageSize( int size, bool enforceQuota );

        //
        // Stats
        //

        bool isCapped() const;

        uint64_t numRecords() const;

        uint64_t dataSize() const;

        int averageObjectSize() const {
            uint64_t n = numRecords();
            if ( n == 0 )
                return 5;
            return static_cast<int>( dataSize() / n );
        }

    private:
        /**
         * same semantics as insertDocument, but doesn't do:
         *  - some user error checks
         *  - adjust padding
         */
        StatusWith<DiskLoc> _insertDocument( const BSONObj& doc, bool enforceQuota );

        // @return 0 for inf., otherwise a number of files
        int largestFileNumberInQuota() const;

        ExtentManager* getExtentManager();
        const ExtentManager* getExtentManager() const;

        int _magic;

        NamespaceString _ns;
        NamespaceDetails* _details;
        Database* _database;
        RecordStore _recordStore;
        CollectionInfoCache _infoCache;
        IndexCatalog _indexCatalog;

        friend class Database;
        friend class FlatIterator;
        friend class CappedIterator;
        friend class IndexCatalog;
    };
    
    class FlipPhase: public BackgroundJob {
    public:
        FlipPhase();
        
        FlipPhase(Database* db, Collection* collection,
                uint64_t scanStableMask, uint64_t scanMask);
        
        virtual ~FlipPhase();
        
        virtual void run();
        
        virtual string name() const { return "FlipPhase"; }
        
    private:
        Database* _db;
        Collection* _collection;
        uint64_t _scanStableMask;
        uint64_t _scanMask;
        
    };

}
