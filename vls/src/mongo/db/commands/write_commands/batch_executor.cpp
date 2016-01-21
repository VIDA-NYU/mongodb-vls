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

#include "mongo/db/commands/write_commands/batch_executor.h"

#include <memory>

#include "mongo/base/error_codes.h"
#include "mongo/db/commands.h"
#include "mongo/db/instance.h"
#include "mongo/db/introspect.h"
#include "mongo/db/lasterror.h"
#include "mongo/db/ops/delete.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/ops/update.h"
#include "mongo/db/ops/update_lifecycle_impl.h"
#include "mongo/db/pagefault.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/replication_server_status.h"
#include "mongo/db/stats/counters.h"
#include "mongo/db/write_concern.h"
#include "mongo/s/collection_metadata.h"
#include "mongo/s/d_logic.h"
#include "mongo/s/shard_key_pattern.h"
#include "mongo/s/write_ops/write_error_detail.h"
#include "mongo/s/write_ops/batched_upsert_detail.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {

    using mongoutils::str::stream;

    WriteBatchExecutor::WriteBatchExecutor( const BSONObj& wc,
                                            Client* client,
                                            OpCounters* opCounters,
                                            LastError* le ) :
        _defaultWriteConcern( wc ),
        _client( client ),
        _opCounters( opCounters ),
        _le( le ),
        _stats( new WriteBatchStats ) {
    }

    static bool buildWCError( const Status& wcStatus,
                              const WriteConcernResult& wcResult,
                              WCErrorDetail* wcError ) {

        // Error reported is either the errmsg or err from wc
        string errMsg;
        if ( !wcStatus.isOK() )
            errMsg = wcStatus.toString();
        else if ( wcResult.err.size() )
            errMsg = wcResult.err;

        if ( errMsg.empty() )
            return false;

        if ( wcStatus.isOK() )
            wcError->setErrCode( ErrorCodes::WriteConcernFailed );
        else
            wcError->setErrCode( wcStatus.code() );

        if ( wcResult.wTimedOut )
            wcError->setErrInfo( BSON( "wtimeout" << true ) );

        wcError->setErrMessage( errMsg );

        return true;
    }

    static WriteErrorDetail* toWriteError( const Status& status ) {

        WriteErrorDetail* error = new WriteErrorDetail;

        // TODO: Complex transform here?
        error->setErrCode( status.code() );
        error->setErrMessage( status.reason() );

        return error;
    }

    void WriteBatchExecutor::executeBatch( const BatchedCommandRequest& request,
                                           BatchedCommandResponse* response ) {

        // TODO: Lift write concern parsing out of this entirely.
        WriteConcernOptions writeConcern;
        Status status = Status::OK();
        if ( request.isWriteConcernSet() ) {
            status = writeConcern.parse( request.getWriteConcern() );
        }
        else {
            status = writeConcern.parse( _defaultWriteConcern );
        }

        if ( !status.isOK() ) {
            response->setErrCode( status.code() );
            response->setErrMessage( status.reason() );
            response->setOk( false );
            dassert( response->isValid(NULL) );
            return;
        }

        bool silentWC = writeConcern.wMode.empty() && writeConcern.wNumNodes == 0
                        && writeConcern.syncMode == WriteConcernOptions::NONE;

        // Apply each batch item, possibly bulking some items together in the write lock.
        // Stops on error if batch is ordered.

        Timer commandTimer;

        OwnedPointerVector<WriteErrorDetail> writeErrorsOwned;
        vector<WriteErrorDetail*>& writeErrors = writeErrorsOwned.mutableVector();

        OwnedPointerVector<BatchedUpsertDetail> upsertedOwned;
        vector<BatchedUpsertDetail*>& upserted = upsertedOwned.mutableVector();

        bulkExecute( request, &upserted, &writeErrors );
        bool staleBatch = !writeErrors.empty()
                          && writeErrors.back()->getErrCode() == ErrorCodes::StaleShardVersion;

        // Send upserted back in response
        if ( upserted.size() ) {
            response->setUpsertDetails( upserted );
            upserted.clear();
        }

        // Send errors back in response
        if ( writeErrors.size() ) {
            response->setErrDetails( writeErrors );
            writeErrors.clear();
        }

        // Send opTime in response
        if ( anyReplEnabled() ) {
            response->setLastOp( _client->getLastOp() );
        }

        // Apply write concern if we had any successful writes
        if ( writeErrors.size() < request.sizeWriteOps() ) {

            _client->curop()->setMessage( "waiting for write concern" );

            WriteConcernResult res;
            status = waitForWriteConcern( writeConcern, _client->getLastOp(), &res );

            WCErrorDetail wcError;
            if ( buildWCError( status, res, &wcError ) ) {
                response->setWriteConcernError( wcError );
            }
        }

        // Set the stats for the response
        response->setN( _stats->numInserted + _stats->numUpserted + _stats->numUpdated
                        + _stats->numDeleted );
        if ( request.getBatchType() == BatchedCommandRequest::BatchType_Update )
            response->setNDocsModified( _stats->numModified );

        // TODO: Audit where we want to queue here - the shardingState calls may block for remote
        // data
        if ( staleBatch ) {

            const BatchedRequestMetadata* requestMetadata = request.getMetadata();
            dassert( requestMetadata );

            // Make sure our shard name is set or is the same as what was set previously
            if ( !shardingState.setShardName( requestMetadata->getShardName() ) ) {

                // If our shard name is stale, our version must have been stale as well
                dassert( writeErrors.size() == request.sizeWriteOps() );
                warning() << "shard name " << requestMetadata->getShardName()
                          << " in batch does not match previously-set shard name "
                          << shardingState.getShardName() << ", not reloading metadata" << endl;
            }
            else {
                // Refresh our shard version
                ChunkVersion latestShardVersion;
                shardingState.refreshMetadataIfNeeded( request.getTargetingNS(),
                                                       requestMetadata->getShardVersion(),
                                                       &latestShardVersion );
            }
        }

        if ( silentWC )
            response->clear();

        response->setOk( true );
        dassert( response->isValid( NULL ) );
    }

    // Translates write item type to wire protocol op code.
    // Helper for WriteBatchExecutor::applyWriteItem().
    static int getOpCode( BatchedCommandRequest::BatchType writeType ) {
        switch ( writeType ) {
        case BatchedCommandRequest::BatchType_Insert:
            return dbInsert;
        case BatchedCommandRequest::BatchType_Update:
            return dbUpdate;
        default:
            dassert( writeType == BatchedCommandRequest::BatchType_Delete );
            return dbDelete;
        }
        return 0;
    }

    static void buildStaleError( const ChunkVersion& shardVersionRecvd,
                                 const ChunkVersion& shardVersionWanted,
                                 WriteErrorDetail* error ) {

        // Write stale error to results
        error->setErrCode( ErrorCodes::StaleShardVersion );

        BSONObjBuilder infoB;
        shardVersionWanted.addToBSON( infoB, "vWanted" );
        error->setErrInfo( infoB.obj() );

        string errMsg = stream() << "stale shard version detected before write, received "
                                 << shardVersionRecvd.toString() << " but local version is "
                                 << shardVersionWanted.toString();
        error->setErrMessage( errMsg );
    }

    static bool checkShardVersion( ShardingState* shardingState,
                                   const BatchedCommandRequest& request,
                                   WriteErrorDetail** error ) {

        const NamespaceString nss( request.getTargetingNS() );
        Lock::assertWriteLocked( nss.ns() );

        ChunkVersion requestShardVersion =
            request.isMetadataSet() && request.getMetadata()->isShardVersionSet() ?
                request.getMetadata()->getShardVersion() : ChunkVersion::IGNORED();

        if ( shardingState->enabled() ) {

            CollectionMetadataPtr metadata = shardingState->getCollectionMetadata( nss.ns() );

            if ( !ChunkVersion::isIgnoredVersion( requestShardVersion ) ) {

                ChunkVersion shardVersion =
                    metadata ? metadata->getShardVersion() : ChunkVersion::UNSHARDED();

                if ( !requestShardVersion.isWriteCompatibleWith( shardVersion ) ) {
                    *error = new WriteErrorDetail;
                    buildStaleError( requestShardVersion, shardVersion, *error );
                    return false;
                }
            }
        }

        return true;
    }

    static void buildUniqueIndexError( const BSONObj& keyPattern,
                                       const BSONObj& indexPattern,
                                       WriteErrorDetail* error ) {
        error->setErrCode( ErrorCodes::CannotCreateIndex );
        string errMsg = stream() << "cannot create unique index over " << indexPattern
                                 << " with shard key pattern " << keyPattern;
        error->setErrMessage( errMsg );
    }

    static bool checkIndexConstraints( ShardingState* shardingState,
                                       const BatchedCommandRequest& request,
                                       WriteErrorDetail** error ) {

        const NamespaceString nss( request.getTargetingNS() );
        Lock::assertWriteLocked( nss.ns() );

        if ( !request.isUniqueIndexRequest() )
            return true;

        if ( shardingState->enabled() ) {

            CollectionMetadataPtr metadata = shardingState->getCollectionMetadata( nss.ns() );

            if ( metadata ) {
                if ( !isUniqueIndexCompatible( metadata->getKeyPattern(),
                                               request.getIndexKeyPattern() ) ) {

                    *error = new WriteErrorDetail;
                    buildUniqueIndexError( metadata->getKeyPattern(),
                                           request.getIndexKeyPattern(),
                                           *error );

                    return false;
                }
            }
        }

        return true;
    }

    //
    // HELPERS FOR CUROP MANAGEMENT AND GLOBAL STATS
    //

    static CurOp* beginCurrentOp( Client* client, const BatchItemRef& currWrite ) {

        // Execute the write item as a child operation of the current operation.
        auto_ptr<CurOp> currentOp( new CurOp( client, client->curop() ) );

        // Set up the child op with more info
        HostAndPort remote =
            client->hasRemote() ? client->getRemote() : HostAndPort( "0.0.0.0", 0 );
        // TODO Modify CurOp "wrapped" constructor to take an opcode, so calling .reset()
        // is unneeded
        currentOp->reset( remote, getOpCode( currWrite.getRequest()->getBatchType() ) );
        currentOp->ensureStarted();
        currentOp->setNS( currWrite.getRequest()->getNS() );

        currentOp->debug().ns = currentOp->getNS();
        currentOp->debug().op = currentOp->getOp();

        if ( currWrite.getOpType() == BatchedCommandRequest::BatchType_Insert ) {
            // No-op for insert, we don't update query or updateobj
        }
        else if ( currWrite.getOpType() == BatchedCommandRequest::BatchType_Update ) {
            currentOp->setQuery( currWrite.getUpdate()->getQuery() );
            currentOp->debug().query = currWrite.getUpdate()->getQuery();
            currentOp->debug().updateobj = currWrite.getUpdate()->getUpdateExpr();
        }
        else {
            dassert( currWrite.getOpType() == BatchedCommandRequest::BatchType_Delete );
            currentOp->setQuery( currWrite.getDelete()->getQuery() );
            currentOp->debug().query = currWrite.getDelete()->getQuery();
        }

        return currentOp.release();
    }

    void WriteBatchExecutor::incOpStats( const BatchItemRef& currWrite ) {

        if ( currWrite.getOpType() == BatchedCommandRequest::BatchType_Insert ) {
            // No-op, for inserts we increment not on the op but once for each write
        }
        else if ( currWrite.getOpType() == BatchedCommandRequest::BatchType_Update ) {
            _opCounters->gotUpdate();
        }
        else {
            dassert( currWrite.getOpType() == BatchedCommandRequest::BatchType_Delete );
            _opCounters->gotDelete();
        }
    }

    void WriteBatchExecutor::incWriteStats( const BatchItemRef& currWrite,
                                            const WriteOpStats& stats,
                                            const WriteErrorDetail* error,
                                            CurOp* currentOp ) {

        if ( currWrite.getOpType() == BatchedCommandRequest::BatchType_Insert ) {
            // We increment batch inserts like individual inserts
            _opCounters->gotInsert();
            _stats->numInserted += stats.n;
            _le->nObjects = stats.n;
            currentOp->debug().ninserted += stats.n;
        }
        else if ( currWrite.getOpType() == BatchedCommandRequest::BatchType_Update ) {
            if ( stats.upsertedID.isEmpty() ) {
                _stats->numUpdated += stats.n;
                _stats->numModified += stats.nModified;
            }
            else {
                ++_stats->numUpserted;
            }

            if ( !error ) {
                _le->recordUpdate( stats.upsertedID.isEmpty(), stats.n, stats.upsertedID );
            }
        }
        else {
            dassert( currWrite.getOpType() == BatchedCommandRequest::BatchType_Delete );
            _stats->numDeleted += stats.n;
            if ( !error ) {
                _le->recordDelete( stats.n );
            }
            currentOp->debug().ndeleted += stats.n;
        }

        // Errors reported in LastError are handled internally in write ops for now
        // TODO: Move error reporting out of write op internals?
    }

    static void finishCurrentOp( Client* client, CurOp* currentOp, WriteErrorDetail* opError ) {

        currentOp->done();
        int executionTime = currentOp->debug().executionTime = currentOp->totalTimeMillis();
        currentOp->debug().recordStats();

        if ( opError ) {
            currentOp->debug().exceptionInfo = ExceptionInfo( opError->getErrMessage(),
                                                              opError->getErrCode() );

            MONGO_TLOG(3) << " Caught Assertion in " << opToString( currentOp->getOp() )
                          << ", continuing " << causedBy( opError->getErrMessage() ) << endl;
        }

        bool logAll = logger::globalLogDomain()->shouldLog( logger::LogSeverity::Debug( 1 ) );
        bool logSlow = executionTime
                       > ( serverGlobalParams.slowMS + currentOp->getExpectedLatencyMs() );

        if ( logAll || logSlow ) {
            MONGO_TLOG(1) << currentOp->debug().report( *currentOp ) << endl;
        }

        if ( currentOp->shouldDBProfile( executionTime ) ) {
            profile( *client, currentOp->getOp(), *currentOp );
        }
    }

    // END HELPERS

    //
    // CORE WRITE OPERATIONS (declaration)
    // These functions write to the database and return stats and zero or one of:
    // - page fault
    // - error
    //

    namespace {

        /**
         * Data structure to safely hold and clean up results of single write operations.
         */
        struct WriteOpResult {

            WriteOpResult() :
                fault( NULL ), error( NULL ) {
            }

            ~WriteOpResult() {
                dassert( !( fault && error ) );
                reset();
            }

            WriteErrorDetail* releaseError() {
                WriteErrorDetail* released = error;
                error = NULL;
                return released;
            }

            void reset() {
                if ( fault )
                    delete fault;
                if ( error )
                    delete error;

                fault = NULL;
                error = NULL;

                stats.reset();
            }

            WriteOpStats stats;

            // Only one of these may be set at once
            PageFaultException* fault;
            WriteErrorDetail* error;
        };

    }

    static void singleInsert( const BatchItemRef& insertItem,
                              const BSONObj& normalInsert,
                              Collection* collection,
                              WriteOpResult* result );

    static void singleCreateIndex( const BatchItemRef& insertItem,
                                   const BSONObj& normalInsert,
                                   Collection* collection,
                                   WriteOpResult* result );

    static void multiUpdate( const BatchItemRef& updateItem, WriteOpResult* result );

    static void multiRemove( const BatchItemRef& removeItem, WriteOpResult* result );

    //
    // WRITE EXECUTION
    // In general, the execXXX operations manage db lock state and stats before dispatching to the
    // core write operations, which are *only* responsible for performing a write and reporting
    // success or failure.
    //

    void WriteBatchExecutor::bulkExecute( const BatchedCommandRequest& request,
                                          std::vector<BatchedUpsertDetail*>* upsertedIds,
                                          std::vector<WriteErrorDetail*>* errors ) {

        if ( request.getBatchType() == BatchedCommandRequest::BatchType_Insert ) {
            execInserts( request, errors );
        }
        else if ( request.getBatchType() == BatchedCommandRequest::BatchType_Update ) {
            for ( size_t i = 0; i < request.sizeWriteOps(); i++ ) {

                WriteErrorDetail* error = NULL;
                BSONObj upsertedId;
                execUpdate( BatchItemRef( &request, i ), &upsertedId, &error );

                if ( !upsertedId.isEmpty() ) {
                    BatchedUpsertDetail* batchUpsertedId = new BatchedUpsertDetail;
                    batchUpsertedId->setIndex( i );
                    batchUpsertedId->setUpsertedID( upsertedId );
                    upsertedIds->push_back( batchUpsertedId );
                }

                if ( error ) {
                    errors->push_back( error );
                    if ( request.getOrdered() )
                        break;
                }
            }
        }
        else {
            dassert( request.getBatchType() == BatchedCommandRequest::BatchType_Delete );
            for ( size_t i = 0; i < request.sizeWriteOps(); i++ ) {

                WriteErrorDetail* error = NULL;
                execRemove( BatchItemRef( &request, i ), &error );

                if ( error ) {
                    errors->push_back( error );
                    if ( request.getOrdered() )
                        break;
                }
            }
        }

        // Fill in stale version errors for unordered batches (update/delete can't do this on own)
        if ( !errors->empty() && !request.getOrdered() ) {

            const WriteErrorDetail* finalError = errors->back();

            if ( finalError->getErrCode() == ErrorCodes::StaleShardVersion ) {
                for ( size_t i = finalError->getIndex() + 1; i < request.sizeWriteOps(); i++ ) {
                    WriteErrorDetail* dupStaleError = new WriteErrorDetail;
                    finalError->cloneTo( dupStaleError );
                    errors->push_back( dupStaleError );
                }
            }
        }
    }

    // Does preprocessing of inserts, special casing for indexes
    // TODO: Simplify this when indexes aren't here anymore
    static StatusWith<BSONObj> normalizeInsert( const BatchItemRef& insertItem ) {

        if ( insertItem.getRequest()->isInsertIndexRequest() ) {

            StatusWith<BSONObj> normalInsert = fixDocumentForInsert( insertItem.getDocument() );
            if ( normalInsert.isOK() && insertItem.getDocument()["ns"].type() != String ) {
                return StatusWith<BSONObj>( ErrorCodes::BadValue, "tried to create an index "
                                            "without specifying namespace" );
            }
            else {
                return normalInsert;
            }
        }
        else {
            return fixDocumentForInsert( insertItem.getDocument() );
        }
    }

    // Goes over the request and preprocesses normalized versions of all the inserts in the request
    static void normalizeInserts( const BatchedCommandRequest& request,
                                  vector<StatusWith<BSONObj> >* normalInserts ) {

        for ( size_t i = 0; i < request.sizeWriteOps(); ++i ) {
            StatusWith<BSONObj> normalInsert = normalizeInsert( BatchItemRef( &request, i ) );
            normalInserts->push_back( normalInsert );
            if ( request.getOrdered() && !normalInsert.isOK() )
                break;
        }
    }

    void WriteBatchExecutor::execInserts( const BatchedCommandRequest& request,
                                          std::vector<WriteErrorDetail*>* errors ) {

        // Bulk insert is a bit different from other bulk operations in that multiple request docs
        // can be processed at once inside the write lock.

        const NamespaceString nss( request.getTargetingNS() );
        scoped_ptr<BatchItemRef> currInsertItem( new BatchItemRef( &request, 0 ) );

        //
        // BEGIN CURRENT OP
        //

        scoped_ptr<CurOp> currentOp( beginCurrentOp( _client, *currInsertItem ) );
        incOpStats( *currInsertItem );

        // Go through our request and do some preprocessing on insert documents outside the lock to
        // validate and put them in a normalized form - i.e. put _id in front and fill in
        // timestamps.  The insert document may also be invalid.
        // TODO:  Might be more efficient to do in batches.
        vector<StatusWith<BSONObj> > normalInserts;
        normalizeInserts( request, &normalInserts );

        WriteErrorDetail* lastOpError = NULL;

        while ( currInsertItem->getItemIndex() < static_cast<int>( request.sizeWriteOps() ) ) {

            WriteOpResult currResult;

            {
                PageFaultRetryableSection pFaultSection;

                ////////////////////////////////////
                Lock::DBWrite writeLock( nss.ns() );
                ////////////////////////////////////

                // Check version inside of write lock

                if ( checkShardVersion( &shardingState, request, &currResult.error )
                     && checkIndexConstraints( &shardingState, request, &currResult.error ) ) {

                    //
                    // Get the collection for the insert
                    //

                    scoped_ptr<Client::Context> writeContext;
                    Collection* collection = NULL;

                    try {
                        // Context once we're locked, to set more details in currentOp()
                        // TODO: better constructor?
                        writeContext.reset( new Client::Context( request.getNS(),
                                                                 storageGlobalParams.dbpath,
                                                                 false /* don't check version */) );

                        Database* database = writeContext->db();
                        dassert( database );
                        collection = database->getCollection( nss.ns() );

                        if ( !collection ) {
                            // Implicitly create if it doesn't exist
                            collection = database->createCollection( nss.ns() );
                            if ( !collection ) {
                                currResult.error =
                                    toWriteError( Status( ErrorCodes::InternalError,
                                                          "could not create collection" ) );
                            }
                        }
                    }
                    catch ( const DBException& ex ) {
                        currResult.error = toWriteError( ex.toStatus() );
                    }

                    //
                    // Perform writes inside write lock
                    //

                    while ( collection
                            && currInsertItem->getItemIndex()
                               < static_cast<int>( request.sizeWriteOps() ) ) {

                        // Get the actual document we want to write, assuming it's valid
                        const StatusWith<BSONObj>& normalInsert = //
                            normalInserts[currInsertItem->getItemIndex()];

                        const BSONObj& normalInsertDoc =
                            normalInsert.getValue().isEmpty() ?
                                currInsertItem->getDocument() : normalInsert.getValue();

                        if ( !normalInsert.isOK() ) {
                            // This insert failed on preprocessing
                            currResult.error = toWriteError( normalInsert.getStatus() );
                        }
                        else if ( !request.isInsertIndexRequest() ) {
                            // Try the insert
                            singleInsert( *currInsertItem,
                                          normalInsertDoc,
                                          collection,
                                          &currResult );
                        }
                        else {
                            // Try the create index
                            singleCreateIndex( *currInsertItem,
                                               normalInsertDoc,
                                               collection,
                                               &currResult );
                        }

                        // Faults release the write lock
                        if ( currResult.fault )
                            break;

                        // In general, we might have stats and errors
                        incWriteStats( *currInsertItem,
                                       currResult.stats,
                                       currResult.error,
                                       currentOp.get() );

                        // Errors release the write lock
                        if ( currResult.error )
                            break;

                        // Increment in the write lock and reset the stats for next time
                        currInsertItem.reset( new BatchItemRef( &request,
                                                                currInsertItem->getItemIndex()
                                                                + 1 ) );
                        currResult.reset();
                    }
                }

            } // END WRITE LOCK

            //
            // Store the current error if it exists
            //

            lastOpError = currResult.error;

            if ( currResult.error ) {

                errors->push_back( currResult.releaseError() );
                errors->back()->setIndex( currInsertItem->getItemIndex() );

                // Break early for ordered batches
                if ( request.getOrdered() )
                    break;
            }

            //
            // Fault or increment
            //

            if ( currResult.fault ) {
                dassert( !lastOpError );
                // Check page fault out of lock
                currResult.fault->touch();
            }
            else {
                // Increment if not a fault
                currInsertItem.reset( new BatchItemRef( &request,
                                                        currInsertItem->getItemIndex() + 1 ) );
            }
        }

        //
        // END CURRENT OP
        //

        finishCurrentOp( _client, currentOp.get(), lastOpError );
    }

    void WriteBatchExecutor::execUpdate( const BatchItemRef& updateItem,
                                         BSONObj* upsertedId,
                                         WriteErrorDetail** error ) {

        // Updates currently do a lot of the lock management internally

        const BatchedCommandRequest& request = *updateItem.getRequest();
        const NamespaceString nss( updateItem.getRequest()->getNS() );

        // BEGIN CURRENT OP
        scoped_ptr<CurOp> currentOp( beginCurrentOp( _client, updateItem ) );
        incOpStats( updateItem );

        WriteOpResult result;

        {
            ///////////////////////////////////////////
            Lock::DBWrite writeLock( nss.ns() );
            ///////////////////////////////////////////

            // Check version once we're locked

            if ( checkShardVersion( &shardingState, request, &result.error ) ) {

                // Context once we're locked, to set more details in currentOp()
                // TODO: better constructor?
                Client::Context writeContext( nss.ns(),
                                              storageGlobalParams.dbpath,
                                              false /* don't check version */);

                multiUpdate( updateItem, &result );

                incWriteStats( updateItem, result.stats, result.error, currentOp.get() );

                if ( !result.stats.upsertedID.isEmpty() ) {
                    *upsertedId = result.stats.upsertedID.getOwned();
                }
            }
        }

        // END CURRENT OP
        finishCurrentOp( _client, currentOp.get(), result.error );

        if ( result.error ) {
            result.error->setIndex( updateItem.getItemIndex() );
            *error = result.releaseError();
        }
    }

    void WriteBatchExecutor::execRemove( const BatchItemRef& removeItem,
                                         WriteErrorDetail** error ) {

        // Removes are similar to updates, but page faults are handled externally

        const BatchedCommandRequest& request = *removeItem.getRequest();
        const NamespaceString nss( removeItem.getRequest()->getNS() );

        // BEGIN CURRENT OP
        scoped_ptr<CurOp> currentOp( beginCurrentOp( _client, removeItem ) );
        incOpStats( removeItem );

        WriteOpResult result;

        while ( true ) {

            {
                // NOTE: Deletes will not fault outside the lock once any data has been written
                PageFaultRetryableSection pFaultSection;

                ///////////////////////////////////////////
                Lock::DBWrite writeLock( nss.ns() );
                ///////////////////////////////////////////

                // Check version once we're locked

                if ( !checkShardVersion( &shardingState, request, &result.error ) ) {
                    // Version error
                    break;
                }

                // Context once we're locked, to set more details in currentOp()
                // TODO: better constructor?
                Client::Context writeContext( nss.ns(),
                                              storageGlobalParams.dbpath,
                                              false /* don't check version */);

                multiRemove( removeItem, &result );

                if ( !result.fault ) {
                    incWriteStats( removeItem, result.stats, result.error, currentOp.get() );
                    break;
                }
            }

            //
            // Check page fault out of lock
            //

            dassert( result.fault );
            result.fault->touch();
            result.reset();
        }

        // END CURRENT OP
        finishCurrentOp( _client, currentOp.get(), result.error );

        if ( result.error ) {
            result.error->setIndex( removeItem.getItemIndex() );
            *error = result.releaseError();
        }
    }

    //
    // IN-DB-LOCK CORE OPERATIONS
    //

    /**
     * Perform a single insert into a collection.  Requires the insert be preprocessed and the
     * collection already has been created.
     *
     * Might fault or error, otherwise populates the result.
     */
    static void singleInsert( const BatchItemRef& insertItem,
                              const BSONObj& normalInsert,
                              Collection* collection,
                              WriteOpResult* result ) {

        const string& insertNS = insertItem.getRequest()->getNS();

        Lock::assertWriteLocked( insertNS );

        try {

            // XXX - are we 100% sure that all !OK statuses do not write a document?
            StatusWith<DiskLoc> status = collection->insertDocument( normalInsert, true );

            if ( !status.isOK() ) {
                result->error = toWriteError( status.getStatus() );
            }
            else {
                logOp( "i", insertNS.c_str(), normalInsert );
                getDur().commitIfNeeded();
                result->stats.n = 1;
            }
        }
        catch ( const PageFaultException& ex ) {
            // TODO: An actual data structure that's not an exception for this
            result->fault = new PageFaultException( ex );
        }
        catch ( const DBException& ex ) {
            result->error = toWriteError( ex.toStatus() );
        }

    }

    /**
     * Perform a single index insert into a collection.  Requires the index descriptor be
     * preprocessed and the collection already has been created.
     *
     * Might fault or error, otherwise populates the result.
     */
    static void singleCreateIndex( const BatchItemRef& insertItem,
                                   const BSONObj& normalIndexDesc,
                                   Collection* collection,
                                   WriteOpResult* result ) {

        const string& indexNS = insertItem.getRequest()->getNS();

        Lock::assertWriteLocked( indexNS );

        try {

            Status status = collection->getIndexCatalog()->createIndex( normalIndexDesc, true );

            if ( status.code() == ErrorCodes::IndexAlreadyExists ) {
                result->stats.n = 0;
            }
            else if ( !status.isOK() ) {
                result->error = toWriteError( status );
            }
            else {
                logOp( "i", indexNS.c_str(), normalIndexDesc );
                result->stats.n = 1;
            }
        }
        catch ( const PageFaultException& ex ) {
            // TODO: An actual data structure that's not an exception for this
            result->fault = new PageFaultException( ex );
        }
        catch ( const DBException& ex ) {
            result->error = toWriteError( ex.toStatus() );
        }
    }

    /**
     * Perform an update operation, which might update multiple documents in the lock.  Dispatches
     * to update code currently to do most of this.
     *
     * Might error, otherwise populates the result.
     */
    static void multiUpdate( const BatchItemRef& updateItem,
                             WriteOpResult* result ) {

        Lock::assertWriteLocked( updateItem.getRequest()->getNS() );

        BSONObj queryObj = updateItem.getUpdate()->getQuery();
        BSONObj updateObj = updateItem.getUpdate()->getUpdateExpr();
        bool multi = updateItem.getUpdate()->getMulti();
        bool upsert = updateItem.getUpdate()->getUpsert();

        bool didInsert = false;
        long long numMatched = 0;
        long long numDocsModified = 0;
        BSONObj resUpsertedID;

        try {

            const NamespaceString requestNs( updateItem.getRequest()->getNS() );
            UpdateRequest request( requestNs );

            request.setQuery( queryObj );
            request.setUpdates( updateObj );
            request.setUpsert( upsert );
            request.setMulti( multi );
            request.setUpdateOpLog();
            // TODO(greg) We need to send if we are ignoring the shard version below,
            // but for now yes
            UpdateLifecycleImpl updateLifecycle( true, requestNs );
            request.setLifecycle( &updateLifecycle );

            UpdateResult res = update( request, &cc().curop()->debug() );

            numDocsModified = res.numDocsModified;
            numMatched = res.numMatched;
            resUpsertedID = res.upserted;

            // We have an _id from an insert
            didInsert = !resUpsertedID.isEmpty();

            result->stats.nModified = didInsert ? 0 : numDocsModified;
            result->stats.n = didInsert ? 1 : numMatched;
            result->stats.upsertedID = resUpsertedID;
        }
        catch ( const DBException& ex ) {
            result->error = toWriteError( ex.toStatus() );
        }
    }

    /**
     * Perform a remove operation, which might remove multiple documents.  Dispatches to remove code
     * currently to do most of this.
     *
     * Might fault or error, otherwise populates the result.
     */
    static void multiRemove( const BatchItemRef& removeItem,
                             WriteOpResult* result ) {

        Lock::assertWriteLocked( removeItem.getRequest()->getNS() );

        try {
            long long n = deleteObjects( removeItem.getRequest()->getNS(),
                                         removeItem.getDelete()->getQuery(),
                                         removeItem.getDelete()->getLimit() == 1, // justOne
                                         true, // logOp
                                         false // god
                                         );

            result->stats.n = n;
        }
        catch ( const PageFaultException& ex ) {
            // TODO: An actual data structure that's not an exception for this
            result->fault = new PageFaultException( ex );
        }
        catch ( const DBException& ex ) {
            result->error = toWriteError( ex.toStatus() );
        }
    }

} // namespace mongo
