// index_catalog.cpp

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

#include "mongo/db/catalog/index_catalog.h"

#include <vector>

#include "mongo/db/audit.h"
#include "mongo/db/background.h"
#include "mongo/db/catalog/index_create.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/curop.h"
#include "mongo/db/field_ref.h"
#include "mongo/db/index_legacy.h"
#include "mongo/db/index/2d_access_method.h"
#include "mongo/db/index/btree_access_method.h"
#include "mongo/db/index/btree_access_method_internal.h"
#include "mongo/db/index/fts_access_method.h"
#include "mongo/db/index/hash_access_method.h"
#include "mongo/db/index/haystack_access_method.h"
#include "mongo/db/index/index_access_method.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/index/s2_access_method.h"
#include "mongo/db/index_names.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/repl/rs.h" // this is ugly
#include "mongo/db/structure/collection.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"

//#include "boost/date_time/posix_time/posix_time.hpp"

namespace mongo {

#define INDEX_CATALOG_MAGIC 283711

    // What's the default version of our indices?
    const int DefaultIndexVersionNumber = 1;

    const BSONObj IndexCatalog::_idObj = BSON( "_id" << 1 );

    // -------------

    IndexCatalog::IndexCatalog( Collection* collection, NamespaceDetails* details )
        : _magic(INDEX_CATALOG_MAGIC), _collection( collection ), _details( details ),
          _descriptorCache( NamespaceDetails::NIndexesMax ),
          _accessMethodCache( NamespaceDetails::NIndexesMax ),
          _forcedBtreeAccessMethodCache( NamespaceDetails::NIndexesMax ) {
        indexMaskMap = std::vector< idx_mask_map_vector >( numIndexesTotal() );
        indexStatusMap = std::vector< idx_status_map_vector >( numIndexesTotal() );
        queueMap = std::vector< queue_map >( numIndexesTotal() );
    }

    IndexCatalog::~IndexCatalog() {
        _checkMagic();
        _magic = 123456;

        for ( unsigned i = 0; i < _descriptorCache.capacity(); i++ ) {
            _deleteCacheEntry(i);
        }

    }

    bool IndexCatalog::ok() const {
        return ( _magic == INDEX_CATALOG_MAGIC );
    }

    void IndexCatalog::_checkMagic() const {
        dassert( _descriptorCache.capacity() == NamespaceDetails::NIndexesMax );
        dassert( _accessMethodCache.capacity() == NamespaceDetails::NIndexesMax );
        dassert( _forcedBtreeAccessMethodCache.capacity() == NamespaceDetails::NIndexesMax );

        if ( ok() )
            return;
        log() << "IndexCatalog::_magic wrong, is : " << _magic;
        fassertFailed(17198);
    }

    bool IndexCatalog::_shouldOverridePlugin(const BSONObj& keyPattern) {
        string pluginName = IndexNames::findPluginName(keyPattern);
        bool known = IndexNames::isKnownName(pluginName);

        const DataFileHeader* dfh = _collection->_database->getFile(0)->getHeader();

        if (dfh->versionMinor == PDFILE_VERSION_MINOR_24_AND_NEWER) {
            // RulesFor24
            // This assert will be triggered when downgrading from a future version that
            // supports an index plugin unsupported by this version.
            uassert(17197, str::stream() << "Invalid index type '" << pluginName << "' "
                    << "in index " << keyPattern,
                    known);
            return false;
        }

        // RulesFor22
        if (!known) {
            log() << "warning: can't find plugin [" << pluginName << "]" << endl;
            return true;
        }

        if (!IndexNames::existedBefore24(pluginName)) {
            warning() << "Treating index " << keyPattern << " as ascending since "
                      << "it was created before 2.4 and '" << pluginName << "' "
                      << "was not a valid type at that time."
                      << endl;
            return true;
        }

        return false;
    }
    
    // ---- VLS ---- //
            
    void IndexCatalog::initializeIndexMaskMap()
    {
        // index mask map
        idx_mask_map_vector indexMaskMapVector;
        // index status map
        idx_status_map_vector indexStatusMapVector;
        idx_status indexStatus (0x0);
        
        for ( unsigned int i = 0; i < _collection->statusMaskMapVector.size(); i++ )
        {
            indexMaskMapVector.push_back( idx_mask_map( ) );
            indexStatusMapVector.push_back( idx_status_map( ) );
            for ( unsigned int j = 0; j < _collection->statusMaskMapVector[i].size(); j++ )
            {                    
                indexMaskMapVector[i].push_back( _collection->statusMaskMapVector[i][j] );
                indexStatusMapVector[i].push_back( indexStatus );
            } 
        }
        
        for ( int i = 0; i < numIndexesTotal(); i++ )
        {
            indexMaskMap[i] = indexMaskMapVector;
            indexStatusMap[i] = indexStatusMapVector;
            queueMap[i] = queue_map();
            
            for (int a = 0; a < (int)indexMaskMapVector.size(); a++)
            {
                indexMaskMap[i][a].reserve( indexMaskMap[i][a].size()*3 );
                indexStatusMap[i][a].reserve( indexStatusMap[i][a].size()*3 );
            }
            
            log() << "Index Mask Map initialized for Index N. " << i << endl;
            log() << "Index Mask Map Size: " << indexMaskMap[i].size() << endl;
        }

        indexMaskMapVector.clear();
    }
    
    IDX_MASK_STATUS IndexCatalog::checkIndexStatus(int &a, ID &id, int &idx_number, uint64_t &scanMask)
    {
        if ( id >= indexMaskMap[idx_number][a].size() )
            return SKIP;

        uint64_t mask = ( (indexMaskMap[idx_number][a][id]) & scanMask );
        idx_status mask_status = indexStatusMap[idx_number][a][id];
        idx_status one (0x1);
        
        if ( ( mask == 0x0 ) && ( mask_status != one ) )
            return UNALTERED;
        else if ( ( mask == 0x0 ) && ( mask_status == one ) )
            return REMOVED;
        else if ( ( mask != 0x0 ) && ( mask_status != one ) )
            return INSERTED;
        else // ( ( mask != 0x0 ) && ( mask_status == one ) )
            return SKIP;
    }
    
    Status IndexCatalog::unindexRecordChronos( int idxNo, const BSONObj& obj, const DiskLoc &loc, bool logIfError ) {
        IndexDescriptor* desc = getDescriptor( idxNo );
        verify( desc );
        IndexAccessMethod* iam = getIndex( desc );
        verify( iam );

        InsertDeleteOptions options;
        options.logIfError = logIfError;

        int64_t removed;
        iam->setCollection(_collection);
        Status status = iam->remove(obj, loc, options, &removed);

        if ( !status.isOK() ) {
            problem() << "Couldn't unindex record " << obj.toString()
                      << " status: " << status.toString();
        }

        return Status::OK();
    }
    
    // ---- VLS ---- //

    string IndexCatalog::_getAccessMethodName(const BSONObj& keyPattern) {
        if ( _shouldOverridePlugin(keyPattern) ) {
            return "";
        }

        return IndexNames::findPluginName(keyPattern);
    }


    // ---------------------------

    Status IndexCatalog::_upgradeDatabaseMinorVersionIfNeeded( const string& newPluginName ) {
        Database* db = _collection->_database;

        DataFileHeader* dfh = db->getFile(0)->getHeader();
        if ( dfh->versionMinor == PDFILE_VERSION_MINOR_24_AND_NEWER ) {
            return Status::OK(); // these checks have already been done
        }

        fassert(16737, dfh->versionMinor == PDFILE_VERSION_MINOR_22_AND_OLDER);

        auto_ptr<Runner> runner( InternalPlanner::collectionScan( db->_indexesName ) );

        BSONObj index;
        Runner::RunnerState state;
        while ( Runner::RUNNER_ADVANCED == (state = runner->getNext(&index, NULL)) ) {
            const BSONObj key = index.getObjectField("key");
            const string plugin = IndexNames::findPluginName(key);
            if ( IndexNames::existedBefore24(plugin) )
                continue;

            const string errmsg = str::stream()
                << "Found pre-existing index " << index << " with invalid type '" << plugin << "'. "
                << "Disallowing creation of new index type '" << newPluginName << "'. See "
                << "http://dochub.mongodb.org/core/index-type-changes"
                ;

            return Status( ErrorCodes::CannotCreateIndex, errmsg );
        }

        if ( Runner::RUNNER_EOF != state ) {
            warning() << "Internal error while reading system.indexes collection";
        }

        getDur().writingInt(dfh->versionMinor) = PDFILE_VERSION_MINOR_24_AND_NEWER;

        return Status::OK();
    }

    Status IndexCatalog::createIndex( BSONObj spec, bool mayInterrupt ) {
        /**
         * There are 2 main variables so(4 possibilies) for how we build indexes
         * variable 1 - size of collection
         * variable 2 - foreground or background
         *
         * size: 0 - we build index in foreground
         * size > 0 - do either fore or back based on ask
         *
         *
         * what it means to create an index
         *  not in an order
         * * system.indexes
         * * entry in collection's NamespaceDetails (IndexDetails)
         * * entry for NamespaceDetails in .ns file for record store
         * * entry in system.namespaces for record store
         * * head entry in IndexDetails populated (?is this required)
         */

        // 1) add entry in system.indexes
        // 2) call into buildAnIndex?

        Status status = okToAddIndex( spec );
        if ( !status.isOK() )
            return status;

        spec = fixIndexSpec( spec );

        // we double check with new index spec
        status = okToAddIndex( spec );
        if ( !status.isOK() )
            return status;


        Database* db = _collection->_database;

        string pluginName = IndexNames::findPluginName( spec["key"].Obj() );
        if ( pluginName.size() ) {
            Status s = _upgradeDatabaseMinorVersionIfNeeded( pluginName );
            if ( !s.isOK() )
                return s;
        }


        Collection* systemIndexes = db->getCollection( db->_indexesName );
        if ( !systemIndexes ) {
            systemIndexes = db->createCollection( db->_indexesName, false, NULL, false );
            verify( systemIndexes );
        }

        StatusWith<DiskLoc> loc = systemIndexes->insertDocument( spec, false );
        if ( !loc.isOK() )
            return loc.getStatus();
        verify( !loc.getValue().isNull() );

        string idxName = spec["name"].valuestr();

        // Set curop description before setting indexBuildInProg, so that there's something
        // commands can find and kill as soon as indexBuildInProg is set. Only set this if it's a
        // killable index, so we don't overwrite commands in currentOp.
        if ( mayInterrupt ) {
            cc().curop()->setQuery( spec );
        }

        IndexBuildBlock indexBuildBlock( this, idxName, loc.getValue() );
        verify( indexBuildBlock.indexDetails() );

        try {
            int idxNo = _details->findIndexByName( idxName, true );
            verify( idxNo >= 0 );

            IndexDetails* id = &_details->idx(idxNo);

            scoped_ptr<IndexDescriptor> desc( new IndexDescriptor( _collection, idxNo,
                                                                   id->info.obj().getOwned() ) );
            auto_ptr<BtreeInMemoryState> btreeState( createInMemory( desc.get() ) );
            buildAnIndex( _collection, btreeState.get(), mayInterrupt );
            indexBuildBlock.success();

            // in case we got any access methods or something like that
            // TEMP until IndexDescriptor has to direct refs
            idxNo = _details->findIndexByName( idxName, true );
            verify( idxNo >= 0 );
            _deleteCacheEntry( idxNo );

            return Status::OK();
        }
        catch (DBException& e) {
            log() << "index build failed."
                  << " spec: " << spec
                  << " error: " << e;

            // in case we got any access methods or something like that
            // TEMP until IndexDescriptor has to direct refs
            int idxNo = _details->findIndexByName( idxName, true );
            verify( idxNo >= 0 );
            _deleteCacheEntry( idxNo );

            ErrorCodes::Error codeToUse = ErrorCodes::fromInt( e.getCode() );
            if ( codeToUse == ErrorCodes::UnknownError )
                return Status( ErrorCodes::InternalError, e.what(), e.getCode() );
            return Status( codeToUse, e.what() );
        }


    }

    IndexCatalog::IndexBuildBlock::IndexBuildBlock( IndexCatalog* catalog,
                                                    const StringData& indexName,
                                                    const DiskLoc& loc )
        : _catalog( catalog ),
          _ns( _catalog->_collection->ns().ns() ),
          _indexName( indexName.toString() ),
          _indexDetails( NULL ) {

        _nsd = _catalog->_collection->details();

        verify( catalog );
        verify( _nsd );
        verify( _catalog->_collection->ok() );
        verify( !loc.isNull() );

        _indexDetails = &_nsd->getNextIndexDetails( _ns.c_str() );

        try {
            // we don't want to kill a half formed IndexDetails, so be carefule
            LOG(1) << "creating index with info @ " << loc;
            getDur().writingDiskLoc( _indexDetails->info ) = loc;
        }
        catch ( DBException& e ) {
            log() << "got exception trying to assign loc to IndexDetails" << e;
            _indexDetails = NULL;
            return;
        }

        try {
            getDur().writingInt( _nsd->_indexBuildsInProgress ) += 1;
        }
        catch ( DBException& e ) {
            log() << "got exception trying to incrementStats _indexBuildsInProgress: " << e;
            _indexDetails = NULL;
            return;
        }

    }

    IndexCatalog::IndexBuildBlock::~IndexBuildBlock() {
        if ( !_indexDetails ) {
            // taken care of already
            return;
        }

        fassert( 17204, _catalog->_collection->ok() );

        int idxNo = _nsd->findIndexByName( _indexName, true );
        fassert( 17205, idxNo >= 0 );

        _catalog->_dropIndex( idxNo );

        _indexDetails = NULL;
    }

    void IndexCatalog::IndexBuildBlock::success() {

        fassert( 17206, _indexDetails );
        BSONObj keyPattern = _indexDetails->keyPattern().getOwned();

        _indexDetails = NULL;

        fassert( 17207, _catalog->_collection->ok() );

        int idxNo = _nsd->findIndexByName( _indexName, true );
        fassert( 17202, idxNo >= 0 );

        // Make sure the newly created index is relocated to nIndexes, if it isn't already there
        if ( idxNo != _nsd->getCompletedIndexCount() ) {
            log() << "switching indexes at position " << idxNo << " and "
                  << _nsd->getCompletedIndexCount() << endl;

            int toIdxNo = _nsd->getCompletedIndexCount();

            _nsd->swapIndex( idxNo, toIdxNo );

            // neither of these should be used in queries yet, so nothing should be caching these
            _catalog->_deleteCacheEntry( idxNo );
            _catalog->_deleteCacheEntry( toIdxNo );

            idxNo = _nsd->getCompletedIndexCount();
        }

        getDur().writingInt( _nsd->_indexBuildsInProgress ) -= 1;
        getDur().writingInt( _nsd->_nIndexes ) += 1;

        _catalog->_collection->infoCache()->addedIndex();

        _catalog->_fixDescriptorCacheNumbers();

        IndexLegacy::postBuildHook( _catalog->_collection, keyPattern );
    }



    Status IndexCatalog::okToAddIndex( const BSONObj& spec ) const {

        const NamespaceString& nss = _collection->ns();


        if ( nss.isSystemDotIndexes() )
            return Status( ErrorCodes::CannotCreateIndex,
                           "cannot create indexes on the system.indexes collection" );

        if ( nss == _collection->_database->_extentFreelistName ) {
            // this isn't really proper, but we never want it and its not an error per se
            return Status( ErrorCodes::IndexAlreadyExists, "cannot index freelist" );
        }

        StringData specNamespace = spec.getStringField("ns");
        if ( specNamespace.size() == 0 )
            return Status( ErrorCodes::CannotCreateIndex,
                           "the index spec needs a 'ns' field'" );

        if ( _collection->ns() != specNamespace )
            return Status( ErrorCodes::CannotCreateIndex,
                           "the index spec ns does not match" );

        // logical name of the index
        const char *name = spec.getStringField("name");
        if ( !name[0] )
            return Status( ErrorCodes::CannotCreateIndex, "no index name specified" );

        string indexNamespace = IndexDetails::indexNamespaceFromObj(spec);
        if ( indexNamespace.length() > 128 )
            return Status( ErrorCodes::CannotCreateIndex,
                           str::stream() << "namespace name generated from index name \"" <<
                           indexNamespace << "\" is too long (128 char max)" );

        BSONObj key = spec.getObjectField("key");
        if ( key.objsize() > 2048 )
            return Status( ErrorCodes::CannotCreateIndex, "index key pattern too large" );

        if ( key.isEmpty() )
            return Status( ErrorCodes::CannotCreateIndex, "index key is empty" );

        if( !validKeyPattern(key) ) {
            return Status( ErrorCodes::CannotCreateIndex,
                           str::stream() << "bad index key pattern " << key );
        }

        // Ensures that the fields on which we are building the index are valid: a field must not
        // begin with a '$' unless it is part of a DBRef or text index, and a field path cannot
        // contain an empty field. If a field cannot be created or updated, it should not be
        // indexable.
        BSONObjIterator it( key );
        while ( it.more() ) {
            BSONElement keyElement = it.next();
            FieldRef keyField( keyElement.fieldName() );

            const size_t numParts = keyField.numParts();
            if ( numParts == 0 ) {
                return Status( ErrorCodes::CannotCreateIndex,
                               str::stream() << "Index key cannot be an empty field." );
            }
            // "$**" is acceptable for a text index.
            if ( str::equals( keyElement.fieldName(), "$**" ) &&
                 keyElement.valuestrsafe() == IndexNames::TEXT )
                continue;


            for ( size_t i = 0; i != numParts; ++i ) {
                const StringData part = keyField.getPart(i);

                // Check if the index key path contains an empty field.
                if ( part.empty() ) {
                    return Status( ErrorCodes::CannotCreateIndex,
                                   str::stream() << "Index key cannot contain an empty field." );
                }

                if ( part[0] != '$' )
                    continue;

                // Check if the '$'-prefixed field is part of a DBRef: since we don't have the
                // necessary context to validate whether this is a proper DBRef, we allow index
                // creation on '$'-prefixed names that match those used in a DBRef.
                const bool mightBePartOfDbRef = (i != 0) &&
                                                (part == "$db" ||
                                                 part == "$id" ||
                                                 part == "$ref");

                if ( !mightBePartOfDbRef ) {
                    return Status( ErrorCodes::CannotCreateIndex,
                                   str::stream() << "Index key contains an illegal field name: "
                                                 << "field name starts with '$'." );
                }
            }
        }

        if ( _collection->isCapped() && spec["dropDups"].trueValue() ) {
            return Status( ErrorCodes::CannotCreateIndex,
                           str::stream() << "Cannot create an index with dropDups=true on a "
                                         << "capped collection, as capped collections do "
                                         << "not allow document removal." );
        }

        if ( !IndexDetails::isIdIndexPattern( key ) ) {
            // for non _id indexes, we check to see if replication has turned off all indexes
            // we _always_ created _id index
            if( theReplSet && !theReplSet->buildIndexes() ) {
                // this is not exactly the right error code, but I think will make the most sense
                return Status( ErrorCodes::IndexAlreadyExists, "no indexes per repl" );
            }
        }


        {
            // Check both existing and in-progress indexes (2nd param = true)
            const int idx = _details->findIndexByName(name, true);
            if (idx >= 0) {
                // index already exists.
                const IndexDetails& indexSpec( _details->idx(idx) );
                BSONObj existingKeyPattern(indexSpec.keyPattern());

                if ( !existingKeyPattern.equal( key ) )
                    return Status( ErrorCodes::CannotCreateIndex,
                                   str::stream() << "Trying to create an index "
                                   << "with same name " << name
                                   << " with different key spec " << key
                                   << " vs existing spec " << existingKeyPattern );

                if ( !indexSpec.areIndexOptionsEquivalent( spec ) )
                    return Status( ErrorCodes::CannotCreateIndex,
                                   str::stream() << "Index with name: " << name
                                   << " already exists with different options" );

                // Index already exists with the same options, so no need to build a new
                // one (not an error). Most likely requested by a client using ensureIndex.
                return Status( ErrorCodes::IndexAlreadyExists, name );
            }
        }

        {
            // Check both existing and in-progress indexes (2nd param = true)
            const int idx = _details->findIndexByKeyPattern(key, true);
            if (idx >= 0) {
                LOG(2) << "index already exists with diff name " << name
                        << ' ' << key << endl;

                const IndexDetails& indexSpec(_details->idx(idx));
                if ( !indexSpec.areIndexOptionsEquivalent( spec ) )
                    return Status( ErrorCodes::CannotCreateIndex,
                                   str::stream() << "Index with pattern: " << key
                                   << " already exists with different options" );

                return Status( ErrorCodes::IndexAlreadyExists, name );
            }
        }

        if ( _details->getTotalIndexCount() >= NamespaceDetails::NIndexesMax ) {
            string s = str::stream() << "add index fails, too many indexes for "
                                     << _collection->ns().ns() << " key:" << key.toString();
            log() << s;
            return Status( ErrorCodes::CannotCreateIndex, s );
        }

        string pluginName = IndexNames::findPluginName( key );
        if ( pluginName.size() ) {
            if ( !IndexNames::isKnownName( pluginName ) )
                return Status( ErrorCodes::CannotCreateIndex,
                               str::stream() << "Unknown index plugin '" << pluginName << "' "
                               << "in index "<< key );

        }

        return Status::OK();
    }

    Status IndexCatalog::ensureHaveIdIndex() {
        if ( _details->isSystemFlagSet( NamespaceDetails::Flag_HaveIdIndex ) )
            return Status::OK();

        dassert( _idObj["_id"].type() == NumberInt );

        BSONObjBuilder b;
        b.append( "name", "_id_" );
        b.append( "ns", _collection->ns().ns() );
        b.append( "key", _idObj );
        BSONObj o = b.done();

        Status s = createIndex( o, false );
        if ( s.isOK() || s.code() == ErrorCodes::IndexAlreadyExists ) {
            _details->setSystemFlag( NamespaceDetails::Flag_HaveIdIndex );
            return Status::OK();
        }

        return s;
    }

    Status IndexCatalog::dropAllIndexes( bool includingIdIndex ) {
        BackgroundOperation::assertNoBgOpInProgForNs( _collection->ns().ns() );

        // there may be pointers pointing at keys in the btree(s).  kill them.
        // TODO: can this can only clear cursors on this index?
        ClientCursor::invalidate( _collection->ns().ns() );

        // make sure nothing in progress
        verify( numIndexesTotal() == numIndexesReady() );

        LOG(4) << "  d->nIndexes was " << numIndexesTotal() << std::endl;

        IndexDetails *idIndex = 0;

        for ( int i = 0; i < numIndexesTotal(); i++ ) {

            if ( !includingIdIndex && _details->idx(i).isIdIndex() ) {
                idIndex = &_details->idx(i);
                continue;
            }

            Status s = dropIndex( i );
            if ( !s.isOK() )
                return s;
            i--;
        }

        if ( idIndex ) {
            verify( numIndexesTotal() == 1 );
        }
        else {
            verify( numIndexesTotal() == 0 );
        }

        _assureSysIndexesEmptied( idIndex );

        return Status::OK();
    }

    Status IndexCatalog::dropIndex( IndexDescriptor* desc ) {
        return dropIndex( desc->getIndexNumber() );
    }

    Status IndexCatalog::dropIndex( int idxNo ) {

        /**
         * IndexState in order
         *  <db>.system.indexes
         *    NamespaceDetails
         *      <db>.system.ns
         */

        // ----- SANITY CHECKS -------------

        verify( idxNo >= 0 );
        verify( idxNo < numIndexesReady() );
        verify( numIndexesReady() == numIndexesTotal() );

        // ------ CLEAR CACHES, ETC -----------

        BackgroundOperation::assertNoBgOpInProgForNs( _collection->ns().ns() );

        return _dropIndex( idxNo );
    }

    Status IndexCatalog::_dropIndex( int idxNo ) {
        verify( idxNo < numIndexesTotal() );
        verify( idxNo >= 0 );

        _checkMagic();
        // there may be pointers pointing at keys in the btree(s).  kill them.
        // TODO: can this can only clear cursors on this index?
        ClientCursor::invalidate( _collection->ns().ns() );

        // wipe out stats
        _collection->infoCache()->reset();

        string indexNamespace = _details->idx( idxNo ).indexNamespace();
        string indexName = _details->idx( idxNo ).indexName();

        // delete my entries first so we don't have invalid pointers lying around
        _deleteCacheEntry(idxNo);

        // --------- START REAL WORK ----------

        audit::logDropIndex( currentClient.get(), indexName, _collection->ns().ns() );

        try {
            _details->clearSystemFlag( NamespaceDetails::Flag_HaveIdIndex );

            // ****   this is the first disk change ****

            // data + system.namespaces
            Status status = _collection->_database->_dropNS( indexNamespace );
            if ( !status.isOK() ) {
                LOG(2) << "IndexDetails::kill(): couldn't drop index " << indexNamespace;
            }

            // all info in the .ns file
            _details->_removeIndexFromMe( idxNo );

            // remove from system.indexes
            int n = _removeFromSystemIndexes( indexName );
            wassert( n == 1 );

        }
        catch ( std::exception& ) {
            // this is bad, and we don't really know state
            // going to leak to make sure things are safe

            log() << "error dropping index: " << indexNamespace
                  << " going to leak some memory to be safe";

            _descriptorCache.clear();
            _accessMethodCache.clear();
            _forcedBtreeAccessMethodCache.clear();

            _collection->_database->_clearCollectionCache( indexNamespace );

            throw;
        }

        _collection->_database->_clearCollectionCache( indexNamespace );

        // now that is really gone can fix arrays

        _checkMagic();

        for ( unsigned i = static_cast<unsigned>(idxNo); i < _descriptorCache.capacity(); i++ ) {
            _deleteCacheEntry(i);
        }

        _fixDescriptorCacheNumbers();

        return Status::OK();
    }

    void IndexCatalog::_deleteCacheEntry( unsigned i ) {
        delete _descriptorCache[i];
        _descriptorCache[i] = NULL;

        delete _accessMethodCache[i];
        _accessMethodCache[i] = NULL;

        delete _forcedBtreeAccessMethodCache[i];
        _forcedBtreeAccessMethodCache[i] = NULL;
    }

    void IndexCatalog::_fixDescriptorCacheNumbers() {

        for ( unsigned i=0; i < _descriptorCache.capacity(); i++ ) {
            if ( !_descriptorCache[i] )
                continue;
            fassert( 17230, static_cast<int>( i ) < numIndexesTotal() );
            IndexDetails& id = _details->idx( i );
            fassert( 17227, _descriptorCache[i]->_indexNumber == static_cast<int>( i ) );
            fassert( 17228, id.info.obj() == _descriptorCache[i]->_infoObj );
        }

    }

    int IndexCatalog::_removeFromSystemIndexes( const StringData& indexName ) {
        BSONObjBuilder b;
        b.append( "ns", _collection->ns() );
        b.append( "name", indexName );
        BSONObj cond = b.done(); // e.g.: { name: "ts_1", ns: "foo.coll" }
        return static_cast<int>( deleteObjects( _collection->_database->_indexesName,
                                                cond, false, false, true ) );
    }

    int IndexCatalog::_assureSysIndexesEmptied( IndexDetails* idIndex ) {
        BSONObjBuilder b;
        b.append("ns", _collection->ns() );
        if ( idIndex ) {
            b.append("name", BSON( "$ne" << idIndex->indexName().c_str() ));
        }
        BSONObj cond = b.done();
        int n = static_cast<int>( deleteObjects( _collection->_database->_indexesName,
                                                 cond, false, false, true) );
        if( n ) {
            warning() << "assureSysIndexesEmptied cleaned up " << n << " entries";
        }
        return n;
    }

    BSONObj IndexCatalog::prepOneUnfinishedIndex() {
        verify( _details->_indexBuildsInProgress > 0 );

        // details.info is always a valid system.indexes entry because DataFileMgr::insert journals
        // creating the index doc and then insert_makeIndex durably assigns its DiskLoc to info.
        // indexBuildsInProgress is set after that, so if it is set, info must be set.
        int offset = numIndexesTotal() - 1;

        BSONObj info = _details->idx(offset).info.obj().getOwned();

        Status s = _dropIndex( offset );

        massert( 17200,
                 str::stream() << "failed to to dropIndex in prepOneUnfinishedIndex: " << s.toString(),
                 s.isOK() );

        return info;
    }

    Status IndexCatalog::blowAwayInProgressIndexEntries() {
        while ( numIndexesInProgress() > 0 ) {
            Status s = dropIndex( numIndexesTotal() - 1 );
            if ( !s.isOK() )
                return s;
        }
        return Status::OK();
    }

    void IndexCatalog::markMultikey( const IndexDescriptor* idx, bool isMultikey ) {
        if ( _details->setIndexIsMultikey( idx->_indexNumber, isMultikey ) )
            _collection->infoCache()->clearQueryCache();
    }

    // ---------------------------

    int IndexCatalog::numIndexesTotal() const {
        return _details->getTotalIndexCount();
    }

    int IndexCatalog::numIndexesReady() const {
        return _details->getCompletedIndexCount();
    }

    IndexDescriptor* IndexCatalog::findIdIndex() {
        for ( int i = 0; i < numIndexesReady(); i++ ) {
            IndexDescriptor* desc = getDescriptor( i );
            if ( desc->isIdIndex() )
                return desc;
        }
        return NULL;
    }

    IndexDescriptor* IndexCatalog::findIndexByName( const StringData& name,
                                                    bool includeUnfinishedIndexes ) {
        int idxNo = _details->findIndexByName( name, includeUnfinishedIndexes );
        if ( idxNo < 0 )
            return NULL;
        return getDescriptor( idxNo );
    }

    IndexDescriptor* IndexCatalog::findIndexByKeyPattern( const BSONObj& key,
                                                          bool includeUnfinishedIndexes ) {
        int idxNo = _details->findIndexByKeyPattern( key, includeUnfinishedIndexes );
        if ( idxNo < 0 )
            return NULL;
        return getDescriptor( idxNo );
    }

    IndexDescriptor* IndexCatalog::findIndexByPrefix( const BSONObj &keyPattern,
                                                      bool requireSingleKey ) {
        IndexDescriptor* best = NULL;

        for ( int i = 0; i < numIndexesReady(); i++ ) {
            IndexDescriptor* desc = getDescriptor( i );

            if ( !keyPattern.isPrefixOf( desc->keyPattern() ) )
                continue;

            if( !_details->isMultikey( i ) )
                return desc;

            if ( !requireSingleKey )
                best = desc;
        }

        return best;
    }

    IndexDescriptor* IndexCatalog::getDescriptor( int idxNo ) {
        _checkMagic();
        verify( idxNo < numIndexesTotal() );

        if ( _descriptorCache[idxNo] )
            return _descriptorCache[idxNo];

        IndexDetails* id = &_details->idx(idxNo);

        if ( static_cast<unsigned>( idxNo ) >= _descriptorCache.size() )
            _descriptorCache.resize( idxNo + 1 );

        _descriptorCache[idxNo] = new IndexDescriptor( _collection, idxNo,
                                                       id->info.obj().getOwned());
        return _descriptorCache[idxNo];
    }

    IndexAccessMethod* IndexCatalog::getBtreeIndex( const IndexDescriptor* desc ) {
        _checkMagic();
        int idxNo = desc->getIndexNumber();

        if ( _forcedBtreeAccessMethodCache[idxNo] ) {
            return _forcedBtreeAccessMethodCache[idxNo];
        }

        BtreeAccessMethod* newlyCreated = new BtreeAccessMethod( createInMemory( desc ) );
        _forcedBtreeAccessMethodCache[idxNo] = newlyCreated;
        return newlyCreated;
    }

    BtreeBasedAccessMethod* IndexCatalog::getBtreeBasedIndex( const IndexDescriptor* desc ) {

        string type = _getAccessMethodName(desc->keyPattern());

        if (IndexNames::HASHED == type ||
            IndexNames::GEO_2DSPHERE == type ||
            IndexNames::TEXT == type ||
            IndexNames::GEO_HAYSTACK == type ||
            "" == type ||
            IndexNames::GEO_2D == type ) {
            IndexAccessMethod* iam = getIndex( desc );
            return dynamic_cast<BtreeBasedAccessMethod*>( iam );
        }

        error() << "getBtreeBasedIndex with a non btree index (" << type << ")";
        verify(0);
        return NULL;
    }


    IndexAccessMethod* IndexCatalog::getIndex( const IndexDescriptor* desc ) {
        _checkMagic();
        int idxNo = desc->getIndexNumber();

        if ( _accessMethodCache[idxNo] ) {
            return _accessMethodCache[idxNo];
        }

        auto_ptr<BtreeInMemoryState> state( createInMemory( desc ) );

        IndexAccessMethod* newlyCreated = 0;

        string type = _getAccessMethodName(desc->keyPattern());

        if (IndexNames::HASHED == type) {
            newlyCreated = new HashAccessMethod( state.release() );
        }
        else if (IndexNames::GEO_2DSPHERE == type) {
            newlyCreated = new S2AccessMethod( state.release() );
        }
        else if (IndexNames::TEXT == type) {
            newlyCreated = new FTSAccessMethod( state.release() );
        }
        else if (IndexNames::GEO_HAYSTACK == type) {
            newlyCreated =  new HaystackAccessMethod( state.release() );
        }
        else if ("" == type) {
            newlyCreated =  new BtreeAccessMethod( state.release() );
        }
        else if (IndexNames::GEO_2D == type) {
            newlyCreated = new TwoDAccessMethod( state.release() );
        }
        else {
            log() << "Can't find index for keypattern " << desc->keyPattern();
            verify(0);
            return NULL;
        }

        _accessMethodCache[idxNo] = newlyCreated;

        return newlyCreated;
    }

    BtreeInMemoryState* IndexCatalog::createInMemory( const IndexDescriptor* descriptor ) {
        int idxNo = _details->findIndexByName( descriptor->indexName(), true );
        verify( idxNo >= 0 );

        Database* db =  _collection->_database;
        NamespaceDetails* nsd = db->namespaceIndex().details( descriptor->indexNamespace() );
        if ( !nsd ) {
            // have to create!
            db->namespaceIndex().add_ns( descriptor->indexNamespace(), DiskLoc(), false );
            nsd = db->namespaceIndex().details( descriptor->indexNamespace() );
            db->_addNamespaceToCatalog( descriptor->indexNamespace(), NULL );
            verify(nsd);
        }

        RecordStore* rs = new RecordStore( descriptor->indexNamespace() );
        rs->init( nsd, _collection->getExtentManager(), false );

        return new BtreeInMemoryState( _collection,
                                       descriptor,
                                       rs,
                                       &_details->idx( idxNo ) );
    }

    // ---------------------------

    Status IndexCatalog::_indexRecord( int idxNo, const BSONObj& obj, const DiskLoc &loc ) {
        IndexDescriptor* desc = getDescriptor( idxNo );
        verify(desc);
        IndexAccessMethod* iam = getIndex( desc );
        verify(iam);

        InsertDeleteOptions options;
        options.logIfError = false;
        options.dupsAllowed =
            ignoreUniqueIndex( desc ) ||
            ( !KeyPattern::isIdKeyPattern(desc->keyPattern()) && !desc->unique() );

        int64_t inserted;
        return iam->insert(obj, loc, options, &inserted);
    }

    Status IndexCatalog::_unindexRecord( int idxNo, const BSONObj& obj, const DiskLoc &loc, bool logIfError ) {
        IndexDescriptor* desc = getDescriptor( idxNo );
        verify( desc );
        IndexAccessMethod* iam = getIndex( desc );
        verify( iam );

        InsertDeleteOptions options;
        options.logIfError = logIfError;

        int64_t removed;
        iam->setCollection(_collection);
        Status status = iam->remove(obj, loc, options, &removed);

        if ( !status.isOK() ) {
            problem() << "Couldn't unindex record " << obj.toString()
                      << " status: " << status.toString();
        }

        return Status::OK();
    }


    void IndexCatalog::indexRecord( const BSONObj& obj, const DiskLoc &loc ) {

        for ( int i = 0; i < numIndexesTotal(); i++ ) {
            try {
                Status s = _indexRecord( i, obj, loc );
                uassert(s.location(), s.reason(), s.isOK() );
                
                if ( _collection->isUserCollection )
                {
                    // inserting index mask
                    //log() << "Inserting Index Mask" << endl;
                    
                    int a = loc.a();
                    uint64_t indexActiveMask = _collection->localIndexActiveMask;
                            
                    tbb::atomic<uint64_t> init_atomic_mask;
                    init_atomic_mask = 0x0;
                    idx_status indexStatus (0x0);
                    
                    if ( i >= (int)indexMaskMap.size() )
                    {
                        indexMaskMap.push_back( idx_mask_map_vector() );
                        indexStatusMap.push_back( idx_status_map_vector() );
                        queueMap.push_back( queue_map() );
                    }
                    
                    if ( a <= (int)indexMaskMap[i].size() )
                    {
                        if ( a == (int)indexMaskMap[i].size() )
                        {
                            indexMaskMap[i].push_back( idx_mask_map( ) );
                            indexStatusMap[i].push_back( idx_status_map( ) );
                            
                            indexMaskMap[i][a].reserve( 800000 );
                            indexStatusMap[i][a].reserve( 800000 );
                        }
                        
                        ID id = loc.id(_collection->ofsMap[a]);
                        tbb::atomic<uint64_t> atomic_mask;
                        atomic_mask = ~( indexActiveMask );
                        
                        idx_mask_map* _indexMaskMap = &indexMaskMap[i][a];
                        idx_status_map* _indexStatusMap = &indexStatusMap[i][a];
                        
                        while (id >= _indexMaskMap->size()) {
                            _indexMaskMap->push_back( init_atomic_mask );
                            _indexStatusMap->push_back( indexStatus );
                        }
                        
                        _indexMaskMap->at(id) = atomic_mask;
                        _indexStatusMap->at(id) = indexStatus;
                    }
                    else
                        log() << "Oops ..." << endl;
                }
            }
            catch ( AssertionException& ae ) {

                LOG(2) << "IndexCatalog::indexRecord failed: " << ae;

                for ( int j = 0; j <= i; j++ ) {
                    try {
                        _unindexRecord( j, obj, loc, false );
                    }
                    catch ( DBException& e ) {
                        LOG(1) << "IndexCatalog::indexRecord rollback failed: " << e;
                    }
                }

                throw;
            }
        }

    }

    void IndexCatalog::unindexRecord( const BSONObj& obj, const DiskLoc& loc, bool noWarn, uint64_t queueID ) {
        int numIndices = numIndexesTotal();
        
        uint64_t indexActiveMask = _collection->localIndexActiveMask;
        idx_status indexStatus (0x1);
        int a = loc.a();
        ID id = _collection->isUserCollection ? loc.id(_collection->ofsMap[a]) : 0;
        
        for (int i = 0; i < numIndices; i++) {
            // If i >= d->nIndexes, it's a background index, and we DO NOT want to log anything.
            bool logIfError = ( i < numIndexesTotal() ) ? !noWarn : false;
            
            if ( !_collection->isUserCollection )
            {
                //boost::posix_time::ptime t1(boost::posix_time::microsec_clock::local_time());
                _unindexRecord( i, obj, loc, logIfError );
                //boost::posix_time::ptime t2(boost::posix_time::microsec_clock::local_time());
                //boost::posix_time::time_duration dt = t2 - t1;
                //log() << "Unindexing takes " << dt.total_nanoseconds() << endl;
            }
            else
            {
                // removing index entry
                //log() << "Removing Index Entry" << endl;
                
                if ( indexActiveMask == 0xFFFFFFFFFFFFFFFF )
                    // no active index scans
                    _unindexRecord( i, obj, loc, logIfError );
                else
                {
                    // do not unindex now
                    queueMap[i][id] = queueID;
                    ( &indexMaskMap[i][a] )->at(id) = ( ( &indexMaskMap[i][a] )->at(id) | indexActiveMask );
                    ( &indexStatusMap[i][a] )->at(id) = indexStatus;
                }
            }
        }

    }

    Status IndexCatalog::checkNoIndexConflicts( const BSONObj &obj ) {
        for ( int idxNo = 0; idxNo < numIndexesTotal(); idxNo++ ) {

            IndexDescriptor* descriptor = getDescriptor( idxNo );

            if ( !descriptor->unique() )
                continue;

            if ( ignoreUniqueIndex(descriptor) )
                continue;

            IndexAccessMethod* iam = getIndex( descriptor );

            InsertDeleteOptions options;
            options.logIfError = false;
            options.dupsAllowed = false;

            UpdateTicket ticket;
            Status ret = iam->validateUpdate(BSONObj(), obj, DiskLoc(), options, &ticket);
            if ( !ret.isOK() )
                return ret;
        }

        return Status::OK();
    }


    bool IndexCatalog::validKeyPattern( const BSONObj& kp ) {
        BSONObjIterator i(kp);
        while( i.more() ) {
            BSONElement e = i.next();
            if( e.type() == Object || e.type() == Array )
                return false;
        }
        return true;
    }

    BSONObj IndexCatalog::fixIndexKey( const BSONObj& key ) {
        if ( IndexDetails::isIdIndexPattern( key ) ) {
            return _idObj;
        }
        if ( key["_id"].type() == Bool && key.nFields() == 1 ) {
            return _idObj;
        }
        return key;
    }


    BSONObj IndexCatalog::fixIndexSpec( const BSONObj& spec ) {
        BSONObj o = IndexLegacy::adjustIndexSpecObject( spec );

        BSONObjBuilder b;

        int v = DefaultIndexVersionNumber;
        if( !o["v"].eoo() ) {
            double vv = o["v"].Number();
            // note (one day) we may be able to fresh build less versions than we can use
            // isASupportedIndexVersionNumber() is what we can use
            uassert(14803, str::stream() << "this version of mongod cannot build new indexes of version number " << vv, 
                    vv == 0 || vv == 1);
            v = (int) vv;
        }
        // idea is to put things we use a lot earlier
        b.append("v", v);

        if( o["unique"].trueValue() )
            b.appendBool("unique", true); // normalize to bool true in case was int 1 or something...

        BSONObj key = fixIndexKey( o["key"].Obj() );
        b.append( "key", key );

        string name = o["name"].String();
        if ( IndexDetails::isIdIndexPattern( key ) ) {
            name = "_id_";
        }
        b.append( "name", name );

        {
            BSONObjIterator i(o);
            while ( i.more() ) {
                BSONElement e = i.next();
                string s = e.fieldName();

                if ( s == "_id" ) {
                    // skip
                }
                else if ( s == "v" || s == "unique" ||
                          s == "key" || s == "name" ) {
                    // covered above
                }
                else if ( s == "key" ) {
                    b.append( "key", fixIndexKey( e.Obj() ) );
                }
                else {
                    b.append(e);
                }
            }
        }

        return b.obj();
    }
}
