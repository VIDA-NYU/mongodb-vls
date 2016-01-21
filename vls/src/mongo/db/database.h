// database.h

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
*    Copyright (C) 2008 10gen Inc.
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
#include <iostream>

#include "mongo/db/namespace_details.h"
#include "mongo/db/storage/extent_manager.h"
#include "mongo/db/storage/record.h"
#include "mongo/db/storage_options.h"
#include "mongo/util/string_map.h"
#include "mongo/db/jsobj.h"

#include <boost/thread/locks.hpp>
#include <boost/thread.hpp>
#include <boost/thread/condition_variable.hpp>

namespace mongo {

    // ---- VLS ---- //

    /* Active Mask locks */
    typedef boost::shared_mutex activeMaskLock;
    typedef boost::unique_lock< activeMaskLock > activeMaskWriteLock;
    typedef boost::shared_lock< activeMaskLock > activeMaskReadLock;
    
    // ---- VLS ---- //

    class Collection;
    class Extent;
    class DataFile;
    class IndexCatalog;
    class IndexDetails;

    /**
     * Database represents a database database
     * Each database database has its own set of files -- dbname.ns, dbname.0, dbname.1, ...
     * NOT memory mapped
    */
    class Database {
    public:
        // you probably need to be in dbHolderMutex when constructing this
        Database(const char *nm, /*out*/ bool& newDb,
                 const string& path = storageGlobalParams.dbpath);

        /* you must use this to close - there is essential code in this method that is not in the ~Database destructor.
           thus the destructor is private.  this could be cleaned up one day...
        */
        static void closeDatabase( const string& db, const string& path );

        const string& name() const { return _name; }
        const string& path() const { return _path; }

        void clearTmpCollections();
        
        // ---- VLS ---- //
        
        /* activeMask controls which vector of bits are active in the database;
           activeMask is a global mask, different from stableMask,
           which is unique for each collection */
        uint64_t activeMask;
        
        /* similar to activeMask, but exclusively for index scans */
        uint64_t indexActiveMask;
        
        /* Active Mask lock */
        activeMaskLock AMLock;
        
        /* Boolean value that indicates if there is an available bit
           for a new scan. */
        bool activeMaskFull;
        
        /* Active Mask Condition Variable;
           used for awaiting scans */
        boost::condition_variable_any activeMaskCondition;
        
        /* Maximum number of scans that can execute concurrently */
        int maxNumberScans;
        
        // ---- VLS ---- //

        /**
         * tries to make sure that this hasn't been deleted
         */
        bool isOk() const { return _magic == 781231; }

        bool isEmpty() { return ! _namespaceIndex.allocated(); }

        /**
         * total file size of Database in bytes
         */
        long long fileSize() const { return _extentManager.fileSize(); }

        int numFiles() const { return _extentManager.numFiles(); }

        /**
         * return file n.  if it doesn't exist, create it
         */
        DataFile* getFile( int n, int sizeNeeded = 0, bool preallocateOnly = false ) {
            _initForWrites();
            return _extentManager.getFile( n, sizeNeeded, preallocateOnly );
        }

        DataFile* addAFile( int sizeNeeded, bool preallocateNextFile ) {
            _initForWrites();
            return _extentManager.addAFile( sizeNeeded, preallocateNextFile );
        }

        /**
         * makes sure we have an extra file at the end that is empty
         * safe to call this multiple times - the implementation will only preallocate one file
         */
        void preallocateAFile() { _extentManager.preallocateAFile(); }

        /**
         * @return true if success.  false if bad level or error creating profile ns
         */
        bool setProfilingLevel( int newLevel , string& errmsg );

        void flushFiles( bool sync ) { return _extentManager.flushFiles( sync ); }

        /**
         * @return true if ns is part of the database
         *         ns=foo.bar, db=foo returns true
         */
        bool ownsNS( const string& ns ) const {
            if ( ! startsWith( ns , _name ) )
                return false;
            return ns[_name.size()] == '.';
        }

        const RecordStats& recordStats() const { return _recordStats; }
        RecordStats& recordStats() { return _recordStats; }

        int getProfilingLevel() const { return _profile; }
        const char* getProfilingNS() const { return _profileName.c_str(); }

        const NamespaceIndex& namespaceIndex() const { return _namespaceIndex; }
        NamespaceIndex& namespaceIndex() { return _namespaceIndex; }

        // TODO: do not think this method should exist, so should try and encapsulate better
        ExtentManager& getExtentManager() { return _extentManager; }
        const ExtentManager& getExtentManager() const { return _extentManager; }

        Status dropCollection( const StringData& fullns );

        Collection* createCollection( const StringData& ns,
                                      bool capped = false,
                                      const BSONObj* options = NULL,
                                      bool allocateDefaultSpace = true );

        /**
         * @param ns - this is fully qualified, which is maybe not ideal ???
         */
        Collection* getCollection( const StringData& ns );

        Collection* getOrCreateCollection( const StringData& ns );

        Status renameCollection( const StringData& fromNS, const StringData& toNS, bool stayTemp );

        /**
         * @return name of an existing database with same text name but different
         * casing, if one exists.  Otherwise the empty string is returned.  If
         * 'duplicates' is specified, it is filled with all duplicate names.
         */
        static string duplicateUncasedName( bool inholderlockalready, const string &name, const string &path, set< string > *duplicates = 0 );

        static Status validateDBName( const StringData& dbname );

        const string& getSystemIndexesName() const { return _indexesName; }
    private:

        void _clearCollectionCache( const StringData& fullns );

        void _clearCollectionCache_inlock( const StringData& fullns );

        ~Database(); // closes files and other cleanup see below.

        void _addNamespaceToCatalog( const StringData& ns, const BSONObj* options );


        /**
         * removes from *.system.namespaces
         * frees extents
         * removes from NamespaceIndex
         * NOT RIGHT NOW, removes cache entry in Database TODO?
         */
        Status _dropNS( const StringData& ns );

        /**
         * make sure namespace is initialized and $freelist is allocated before
         * doing anything that will write
         */
        void _initForWrites() {
            _namespaceIndex.init();
            if ( !_extentManager.hasFreeList() ) {
                _initExtentFreeList();
            }
        }

        void _initExtentFreeList();

        /**
         * @throws DatabaseDifferCaseCode if the name is a duplicate based on
         * case insensitive matching.
         */
        void checkDuplicateUncasedNames(bool inholderlockalready) const;

        void openAllFiles();

        Status _renameSingleNamespace( const StringData& fromNS, const StringData& toNS,
                                       bool stayTemp );

        const string _name; // "alleyinsider"
        const string _path; // "/data/db"

        NamespaceIndex _namespaceIndex;
        ExtentManager _extentManager;

        const string _profileName; // "alleyinsider.system.profile"
        const string _namespacesName; // "alleyinsider.system.namespaces"
        const string _indexesName; // "alleyinsider.system.indexes"
        const string _extentFreelistName;

        RecordStats _recordStats;
        int _profile; // 0=off.

        int _magic; // used for making sure the object is still loaded in memory

        // TODO: make sure deletes go through
        // this in some ways is a dupe of _namespaceIndex
        // but it points to a much more useful data structure
        typedef StringMap< Collection* > CollectionMap;
        CollectionMap _collections;
        mutex _collectionLock;

        friend class Collection;
        friend class NamespaceDetails;
        friend class IndexDetails;
        friend class IndexCatalog;
    };

} // namespace mongo
