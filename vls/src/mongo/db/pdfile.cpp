// pdfile.cpp

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

/*
todo:
_ table scans must be sequential, not next/prev pointers
_ coalesce deleted
_ disallow system* manipulations from the database.
*/

#include "mongo/pch.h"

#include "mongo/db/pdfile.h"

#include <algorithm>
#include <boost/filesystem/operations.hpp>
#include <boost/optional/optional.hpp>
#include <list>

#include "mongo/base/counter.h"
#include "mongo/base/owned_pointer_vector.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/auth_index_d.h"
#include "mongo/db/auth/user_document_parser.h"
#include "mongo/db/pdfile_private.h"
#include "mongo/db/background.h"
#include "mongo/db/structure/btree/btree.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/cloner.h"
#include "mongo/db/commands/server_status.h"
#include "mongo/db/curop-inl.h"
#include "mongo/db/db.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/extsort.h"
#include "mongo/db/index_legacy.h"
#include "mongo/db/index_names.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/instance.h"
#include "mongo/db/kill_current_op.h"
#include "mongo/db/lasterror.h"
#include "mongo/db/memconcept.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/repl/is_master.h"
#include "mongo/db/sort_phase_one.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/storage_options.h"
#include "mongo/db/structure/collection.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/file.h"
#include "mongo/util/file_allocator.h"
#include "mongo/util/hashtab.h"
#include "mongo/util/mmap.h"
#include "mongo/util/processinfo.h"
#include "mongo/db/stats/timer_stats.h"
#include "mongo/db/stats/counters.h"

namespace mongo {

    bool isValidNS( const StringData& ns ) {
        // TODO: should check for invalid characters

        size_t idx = ns.find( '.' );
        if ( idx == string::npos )
            return false;

        if ( idx == ns.size() - 1 )
            return false;

        return true;
    }

    // TODO SERVER-4328
    bool inDBRepair = false;
    struct doingRepair {
        doingRepair() {
            verify( ! inDBRepair );
            inDBRepair = true;
        }
        ~doingRepair() {
            inDBRepair = false;
        }
    };

    /* ----------------------------------------- */
    const char FREELIST_NS[] = ".$freelist";
    string pidfilepath;

    DatabaseHolder _dbHolder;
    int MAGIC = 0x1000;

    DatabaseHolder& dbHolderUnchecked() {
        return _dbHolder;
    }

    void ensureIdIndexForNewNs( Collection* collection ) {
        if ( collection->ns().isSystem() && !legalClientSystemNS( collection->ns().ns(), false ) )
            return;

        uassertStatusOK( collection->getIndexCatalog()->ensureHaveIdIndex() );
    }

    string getDbContext() {
        stringstream ss;
        Client * c = currentClient.get();
        if ( c ) {
            Client::Context * cx = c->getContext();
            if ( cx ) {
                Database *database = cx->db();
                if ( database ) {
                    ss << database->name() << ' ';
                    ss << cx->ns() << ' ';
                }
            }
        }
        return ss.str();
    }

    /*---------------------------------------------------------------------*/

    // inheritable class to implement an operation that may be applied to all
    // files in a database using _applyOpToDataFiles()
    class FileOp {
    public:
        virtual ~FileOp() {}
        // Return true if file exists and operation successful
        virtual bool apply( const boost::filesystem::path &p ) = 0;
        virtual const char * op() const = 0;
    };

    void _applyOpToDataFiles(const char *database, FileOp &fo, bool afterAllocator = false,
                             const string& path = storageGlobalParams.dbpath);

    void _deleteDataFiles(const char *database) {
        if (storageGlobalParams.directoryperdb) {
            FileAllocator::get()->waitUntilFinished();
            MONGO_ASSERT_ON_EXCEPTION_WITH_MSG(
                    boost::filesystem::remove_all(
                        boost::filesystem::path(storageGlobalParams.dbpath) / database),
                                                "delete data files with a directoryperdb");
            return;
        }
        class : public FileOp {
            virtual bool apply( const boost::filesystem::path &p ) {
                return boost::filesystem::remove( p );
            }
            virtual const char * op() const {
                return "remove";
            }
        } deleter;
        _applyOpToDataFiles( database, deleter, true );
    }

    bool _userCreateNS(const char *ns, const BSONObj& options, string& err, bool *deferIdIndex) {
        LOG(1) << "create collection " << ns << ' ' << options << endl;

        Database* db = cc().database();

        Collection* collection = db->getCollection( ns );

        if ( collection ) {
            err = "collection already exists";
            return false;
        }

        long long size = Extent::initialSize(128);
        {
            BSONElement e = options.getField("size");
            if ( e.isNumber() ) {
                size = e.numberLong();
                uassert( 10083 , "create collection invalid size spec", size >= 0 );

                size += 0xff;
                size &= 0xffffffffffffff00LL;
                if ( size < Extent::minSize() )
                    size = Extent::minSize();
            }
        }

        bool newCapped = false;
        long long mx = 0;
        if( options["capped"].trueValue() ) {
            newCapped = true;
            BSONElement e = options.getField("max");
            if ( e.isNumber() ) {
                mx = e.numberLong();
                uassert( 16495,
                         "max in a capped collection has to be < 2^31 or not set",
                         NamespaceDetails::validMaxCappedDocs(&mx) );
            }
        }


        collection = db->createCollection( ns,
                                           options["capped"].trueValue(),
                                           &options,
                                           false ); // we do it ourselves below
        verify( collection );

        // $nExtents just for debug/testing.
        BSONElement e = options.getField( "$nExtents" );

        if ( e.type() == Array ) {
            // We create one extent per array entry, with size specified
            // by the array value.
            BSONObjIterator i( e.embeddedObject() );
            while( i.more() ) {
                BSONElement e = i.next();
                int size = int( e.number() );
                verify( size <= 0x7fffffff );
                // $nExtents is just for testing - always allocate new extents
                // rather than reuse existing extents so we have some predictibility
                // in the extent size used by our tests
                collection->increaseStorageSize( (int)size, false );
            }
        }
        else if ( int( e.number() ) > 0 ) {
            // We create '$nExtents' extents, each of size 'size'.
            int nExtents = int( e.number() );
            verify( size <= 0x7fffffff );
            for ( int i = 0; i < nExtents; ++i ) {
                verify( size <= 0x7fffffff );
                // $nExtents is just for testing - always allocate new extents
                // rather than reuse existing extents so we have some predictibility
                // in the extent size used by our tests
                collection->increaseStorageSize( (int)size, false );
            }
        }
        else {
            // This is the non test case, where we don't have a $nExtents spec.
            while ( size > 0 ) {
                const int max = Extent::maxSize();
                const int min = Extent::minSize();
                int desiredExtentSize = static_cast<int> (size > max ? max : size);
                desiredExtentSize = static_cast<int> (desiredExtentSize < min ? min : desiredExtentSize);

                desiredExtentSize &= 0xffffff00;
                Extent* e = collection->increaseStorageSize( (int)desiredExtentSize, true );
                size -= e->length;
            }
        }

        NamespaceDetails *d = nsdetails(ns);
        verify(d);

        bool ensure = true;

        // respect autoIndexId if set. otherwise, create an _id index for all colls, except for
        // capped ones in local w/o autoIndexID (reason for the exception is for the oplog and
        //  non-replicated capped colls)
        if( options.hasField( "autoIndexId" ) ||
            (newCapped && nsToDatabase( ns ) == "local" ) ) {
            ensure = options.getField( "autoIndexId" ).trueValue();
        }

        if( ensure ) {
            if( deferIdIndex )
                *deferIdIndex = true;
            else
                ensureIdIndexForNewNs( collection );
        }

        if ( mx > 0 )
            d->setMaxCappedDocs( mx );

        if ( options["flags"].numberInt() ) {
            d->replaceUserFlags( options["flags"].numberInt() );
        }

        return true;
    }

    /** { ..., capped: true, size: ..., max: ... }
        @param deferIdIndex - if not not, defers id index creation.  sets the bool value to true if we wanted to create the id index.
        @return true if successful
    */
    bool userCreateNS(const char *ns, BSONObj options, string& err, bool logForReplication, bool *deferIdIndex) {
        const char *coll = strchr( ns, '.' ) + 1;
        massert(10356 ,
                str::stream() << "invalid ns: " << ns,
                NamespaceString::validCollectionComponent(ns));

        bool ok = _userCreateNS(ns, options, err, deferIdIndex);
        if ( logForReplication && ok ) {
            if ( options.getField( "create" ).eoo() ) {
                BSONObjBuilder b;
                b << "create" << coll;
                b.appendElements( options );
                options = b.obj();
            }
            string logNs = nsToDatabase(ns) + ".$cmd";
            logOp("c", logNs.c_str(), options);
        }
        return ok;
    }

    /** add a record to the end of the linked list chain within this extent. 
        require: you must have already declared write intent for the record header.        
    */
    void addRecordToRecListInExtent(Record *r, DiskLoc loc) {
        dassert( loc.rec() == r );
        Extent *e = r->myExtent(loc);
        if ( e->lastRecord.isNull() ) {
            Extent::FL *fl = getDur().writing(e->fl());
            fl->firstRecord = fl->lastRecord = loc;
            r->prevOfs() = r->nextOfs() = DiskLoc::NullOfs;
        }
        else {
            Record *oldlast = e->lastRecord.rec();
            r->prevOfs() = e->lastRecord.getOfs();
            r->nextOfs() = DiskLoc::NullOfs;
            getDur().writingInt(oldlast->nextOfs()) = loc.getOfs();
            getDur().writingDiskLoc(e->lastRecord) = loc;
        }
    }

    NOINLINE_DECL DiskLoc outOfSpace(const char* ns, NamespaceDetails* d, int lenWHdr, bool god) {
        DiskLoc loc;
        if ( d->isCapped() ) {
            // size capped doesn't grow
            return loc;
        }

        LOG(1) << "allocating new extent for " << ns << " padding:" << d->paddingFactor() << " lenWHdr: " << lenWHdr << endl;

        Collection* collection = cc().database()->getCollection( ns );
        verify( collection );

        collection->increaseStorageSize( Extent::followupSize(lenWHdr, d->lastExtentSize()), !god );

        loc = d->alloc(ns, lenWHdr);
        if ( !loc.isNull() ) {
            // got on first try
            return loc;
        }

        log() << "warning: alloc() failed after allocating new extent. "
              << "lenWHdr: " << lenWHdr << " last extent size:"
              << d->lastExtentSize() << "; trying again" << endl;

        for ( int z = 0; z < 10 && lenWHdr > d->lastExtentSize(); z++ ) {
            log() << "try #" << z << endl;
            collection->increaseStorageSize( Extent::followupSize(lenWHdr, d->lastExtentSize()), !god);

            loc = d->alloc(ns, lenWHdr);
            if ( ! loc.isNull() )
                break;
        }

        return loc;
    }

    /** used by insert and also compact
      * @return null loc if out of space
      * XXX-ERH
      */
    DiskLoc allocateSpaceForANewRecord(const char* ns, NamespaceDetails* d, int lenWHdr, bool god) {
        DiskLoc loc = d->alloc(ns, lenWHdr);
        if ( loc.isNull() ) {
            loc = outOfSpace(ns, d, lenWHdr, god);
        }
        return loc;
    }
}

#include "clientcursor.h"

namespace mongo {

    void dropAllDatabasesExceptLocal() {
        Lock::GlobalWrite lk;

        vector<string> n;
        getDatabaseNames(n);
        if( n.size() == 0 ) return;
        log() << "dropAllDatabasesExceptLocal " << n.size() << endl;
        for( vector<string>::iterator i = n.begin(); i != n.end(); i++ ) {
            if( *i != "local" ) {
                Client::Context ctx(*i);
                dropDatabase(*i);
            }
        }
    }

    void dropDatabase(const std::string& db) {
        LOG(1) << "dropDatabase " << db << endl;
        Lock::assertWriteLocked(db);
        Database *d = cc().database();
        verify( d );
        verify( d->name() == db );

        BackgroundOperation::assertNoBgOpInProgForDb(d->name().c_str());

        audit::logDropDatabase( currentClient.get(), db );

        // Not sure we need this here, so removed.  If we do, we need to move it down
        // within other calls both (1) as they could be called from elsewhere and
        // (2) to keep the lock order right - groupcommitmutex must be locked before
        // mmmutex (if both are locked).
        //
        //  RWLockRecursive::Exclusive lk(MongoFile::mmmutex);

        getDur().syncDataAndTruncateJournal();

        Database::closeDatabase( d->name(), d->path() );
        d = 0; // d is now deleted

        _deleteDataFiles( db.c_str() );
    }

    typedef boost::filesystem::path Path;

    void boostRenameWrapper( const Path &from, const Path &to ) {
        try {
            boost::filesystem::rename( from, to );
        }
        catch ( const boost::filesystem::filesystem_error & ) {
            // boost rename doesn't work across partitions
            boost::filesystem::copy_file( from, to);
            boost::filesystem::remove( from );
        }
    }

    // back up original database files to 'temp' dir
    void _renameForBackup( const char *database, const Path &reservedPath ) {
        Path newPath( reservedPath );
        if (storageGlobalParams.directoryperdb)
            newPath /= database;
        class Renamer : public FileOp {
        public:
            Renamer( const Path &newPath ) : newPath_( newPath ) {}
        private:
            const boost::filesystem::path &newPath_;
            virtual bool apply( const Path &p ) {
                if ( !boost::filesystem::exists( p ) )
                    return false;
                boostRenameWrapper( p, newPath_ / ( p.leaf().string() + ".bak" ) );
                return true;
            }
            virtual const char * op() const {
                return "renaming";
            }
        } renamer( newPath );
        _applyOpToDataFiles( database, renamer, true );
    }

    // move temp files to standard data dir
    void _replaceWithRecovered( const char *database, const char *reservedPathString ) {
        Path newPath(storageGlobalParams.dbpath);
        if (storageGlobalParams.directoryperdb)
            newPath /= database;
        class Replacer : public FileOp {
        public:
            Replacer( const Path &newPath ) : newPath_( newPath ) {}
        private:
            const boost::filesystem::path &newPath_;
            virtual bool apply( const Path &p ) {
                if ( !boost::filesystem::exists( p ) )
                    return false;
                boostRenameWrapper( p, newPath_ / p.leaf() );
                return true;
            }
            virtual const char * op() const {
                return "renaming";
            }
        } replacer( newPath );
        _applyOpToDataFiles( database, replacer, true, reservedPathString );
    }

    // generate a directory name for storing temp data files
    Path uniqueReservedPath( const char *prefix ) {
        Path repairPath = Path(storageGlobalParams.repairpath);
        Path reservedPath;
        int i = 0;
        bool exists = false;
        do {
            stringstream ss;
            ss << prefix << "_repairDatabase_" << i++;
            reservedPath = repairPath / ss.str();
            MONGO_ASSERT_ON_EXCEPTION( exists = boost::filesystem::exists( reservedPath ) );
        }
        while ( exists );
        return reservedPath;
    }

    boost::intmax_t dbSize( const char *database ) {
        class SizeAccumulator : public FileOp {
        public:
            SizeAccumulator() : totalSize_( 0 ) {}
            boost::intmax_t size() const {
                return totalSize_;
            }
        private:
            virtual bool apply( const boost::filesystem::path &p ) {
                if ( !boost::filesystem::exists( p ) )
                    return false;
                totalSize_ += boost::filesystem::file_size( p );
                return true;
            }
            virtual const char *op() const {
                return "checking size";
            }
            boost::intmax_t totalSize_;
        };
        SizeAccumulator sa;
        _applyOpToDataFiles( database, sa );
        return sa.size();
    }

    bool repairDatabase( string dbNameS , string &errmsg,
                         bool preserveClonedFilesOnFailure, bool backupOriginalFiles ) {
        doingRepair dr;
        dbNameS = nsToDatabase( dbNameS );
        const char * dbName = dbNameS.c_str();

        stringstream ss;
        ss << "localhost:" << serverGlobalParams.port;
        string localhost = ss.str();

        problem() << "repairDatabase " << dbName << endl;
        verify( cc().database()->name() == dbName );
        verify(cc().database()->path() == storageGlobalParams.dbpath);

        BackgroundOperation::assertNoBgOpInProgForDb(dbName);

        getDur().syncDataAndTruncateJournal(); // Must be done before and after repair

        boost::intmax_t totalSize = dbSize( dbName );
        boost::intmax_t freeSize = File::freeSpace(storageGlobalParams.repairpath);
        if ( freeSize > -1 && freeSize < totalSize ) {
            stringstream ss;
            ss << "Cannot repair database " << dbName << " having size: " << totalSize
               << " (bytes) because free disk space is: " << freeSize << " (bytes)";
            errmsg = ss.str();
            problem() << errmsg << endl;
            return false;
        }

        killCurrentOp.checkForInterrupt();

        Path reservedPath =
            uniqueReservedPath( ( preserveClonedFilesOnFailure || backupOriginalFiles ) ?
                                "backup" : "_tmp" );
        MONGO_ASSERT_ON_EXCEPTION( boost::filesystem::create_directory( reservedPath ) );
        string reservedPathString = reservedPath.string();

        bool res;
        {
            // clone to temp location, which effectively does repair
            Client::Context ctx( dbName, reservedPathString );
            verify( ctx.justCreated() );


            CloneOptions cloneOptions;
            cloneOptions.fromDB = dbName;
            cloneOptions.logForRepl = false;
            cloneOptions.slaveOk = false;
            cloneOptions.useReplAuth = false;
            cloneOptions.snapshot = false;
            cloneOptions.mayYield = false;
            cloneOptions.mayBeInterrupted = true;
            res = Cloner::cloneFrom(ctx, localhost, cloneOptions, errmsg );

            Database::closeDatabase( dbName, reservedPathString.c_str() );
        }

        getDur().syncDataAndTruncateJournal(); // Must be done before and after repair
        MongoFile::flushAll(true); // need both in case journaling is disabled

        if ( !res ) {
            errmsg = str::stream() << "clone failed for " << dbName << " with error: " << errmsg;
            problem() << errmsg << endl;

            if ( !preserveClonedFilesOnFailure )
                MONGO_ASSERT_ON_EXCEPTION( boost::filesystem::remove_all( reservedPath ) );

            return false;
        }

        Client::Context ctx( dbName );
        Database::closeDatabase(dbName, storageGlobalParams.dbpath);

        if ( backupOriginalFiles ) {
            _renameForBackup( dbName, reservedPath );
        }
        else {
            _deleteDataFiles( dbName );
            MONGO_ASSERT_ON_EXCEPTION(
                    boost::filesystem::create_directory(Path(storageGlobalParams.dbpath) / dbName));
        }

        _replaceWithRecovered( dbName, reservedPathString.c_str() );

        if ( !backupOriginalFiles )
            MONGO_ASSERT_ON_EXCEPTION( boost::filesystem::remove_all( reservedPath ) );

        return true;
    }

    void _applyOpToDataFiles( const char *database, FileOp &fo, bool afterAllocator, const string& path ) {
        if ( afterAllocator )
            FileAllocator::get()->waitUntilFinished();
        string c = database;
        c += '.';
        boost::filesystem::path p(path);
        if (storageGlobalParams.directoryperdb)
            p /= database;
        boost::filesystem::path q;
        q = p / (c+"ns");
        bool ok = false;
        MONGO_ASSERT_ON_EXCEPTION( ok = fo.apply( q ) );
        if ( ok ) {
            LOG(2) << fo.op() << " file " << q.string() << endl;
        }
        int i = 0;
        int extra = 10; // should not be necessary, this is defensive in case there are missing files
        while ( 1 ) {
            verify( i <= DiskLoc::MaxFiles );
            stringstream ss;
            ss << c << i;
            q = p / ss.str();
            MONGO_ASSERT_ON_EXCEPTION( ok = fo.apply(q) );
            if ( ok ) {
                if ( extra != 10 ) {
                    LOG(1) << fo.op() << " file " << q.string() << endl;
                    log() << "  _applyOpToDataFiles() warning: extra == " << extra << endl;
                }
            }
            else if ( --extra <= 0 )
                break;
            i++;
        }
    }

} // namespace mongo
