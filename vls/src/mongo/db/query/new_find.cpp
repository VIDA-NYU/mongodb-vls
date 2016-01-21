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

#include "mongo/db/query/new_find.h"

#include "mongo/client/dbclientinterface.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/commands.h"
#include "mongo/db/exec/filter.h"
#include "mongo/db/exec/oplogstart.h"
#include "mongo/db/keypattern.h"
#include "mongo/db/kill_current_op.h"
#include "mongo/db/query/find_constants.h"
#include "mongo/db/query/get_runner.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/query/qlog.h"
#include "mongo/db/query/query_planner_params.h"
#include "mongo/db/query/single_solution_runner.h"
#include "mongo/db/query/type_explain.h"
#include "mongo/db/repl/repl_reads_ok.h"
#include "mongo/db/server_options.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/storage_options.h"
#include "mongo/db/structure/collection.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/d_logic.h"
#include "mongo/s/stale_exception.h"
#include "mongo/util/mongoutils/str.h"

namespace mongo {
    // The .h for this in find_constants.h.
    const int32_t MaxBytesToReturnToClientAtOnce = 4 * 1024 * 1024;
}  // namespace mongo

namespace {

    // TODO: Remove this or use it.
    bool hasIndexSpecifier(const mongo::LiteParsedQuery& pq) {
        return !pq.getHint().isEmpty() || !pq.getMin().isEmpty() || !pq.getMax().isEmpty();
    }

    /**
     * Quote:
     * if ntoreturn is zero, we return up to 101 objects.  on the subsequent getmore, there
     * is only a size limit.  The idea is that on a find() where one doesn't use much results,
     * we don't return much, but once getmore kicks in, we start pushing significant quantities.
     *
     * The n limit (vs. size) is important when someone fetches only one small field from big
     * objects, which causes massive scanning server-side.
     */
    bool enoughForFirstBatch(const mongo::LiteParsedQuery& pq, int n, int len) {
        if (0 == pq.getNumToReturn()) {
            return (len > 1024 * 1024) || n >= 101;
        }
        return n >= pq.getNumToReturn() || len > mongo::MaxBytesToReturnToClientAtOnce;
    }

    bool enough(const mongo::LiteParsedQuery& pq, int n) {
        if (0 == pq.getNumToReturn()) { return false; }
        return n >= pq.getNumToReturn();
    }

    bool enoughForExplain(const mongo::LiteParsedQuery& pq, long long n) {
        if (pq.wantMore() || 0 == pq.getNumToReturn()) { return false; }
        return n >= pq.getNumToReturn();
    }

}  // namespace

namespace mongo {

    // XXX clean up
    static bool runCommands(const char *ns, BSONObj& jsobj, CurOp& curop, BufBuilder &b, BSONObjBuilder& anObjBuilder, bool fromRepl, int queryOptions) {
        try {
            return _runCommands(ns, jsobj, b, anObjBuilder, fromRepl, queryOptions);
        }
        catch( SendStaleConfigException& ){
            throw;
        }
        catch ( AssertionException& e ) {
            verify( e.getCode() != SendStaleConfigCode && e.getCode() != RecvStaleConfigCode );

            Command::appendCommandStatus(anObjBuilder, e.toStatus());
            curop.debug().exceptionInfo = e.getInfo();
        }
        BSONObj x = anObjBuilder.done();
        b.appendBuf((void*) x.objdata(), x.objsize());
        return true;
    }

    /**
     * Also called by db/ops/query.cpp.  This is the new getMore entry point.
     */
    QueryResult* newGetMore(const char* ns, int ntoreturn, long long cursorid, CurOp& curop,
                            int pass, bool& exhaust, bool* isCursorAuthorized) {
        exhaust = false;
        int bufSize = 512 + sizeof(QueryResult) + MaxBytesToReturnToClientAtOnce;

        BufBuilder bb(bufSize);
        bb.skip(sizeof(QueryResult));

        // This is a read lock.
        scoped_ptr<Client::ReadContext> ctx(new Client::ReadContext(ns));

        QLOG() << "running getMore in new system, cursorid " << cursorid << endl;

        // This checks to make sure the operation is allowed on a replicated node.  Since we are not
        // passing in a query object (necessary to check SlaveOK query option), the only state where
        // reads are allowed is PRIMARY (or master in master/slave).  This function uasserts if
        // reads are not okay.
        replVerifyReadsOk();

        // A pin performs a CC lookup and if there is a CC, increments the CC's pin value so it
        // doesn't time out.  Also informs ClientCursor that there is somebody actively holding the
        // CC, so don't delete it.
        ClientCursorPin ccPin(cursorid);
        ClientCursor* cc = ccPin.c();

        // These are set in the QueryResult msg we return.
        int resultFlags = ResultFlag_AwaitCapable;

        int numResults = 0;
        int startingResult = 0;

        if (NULL == cc) {
            cursorid = 0;
            resultFlags = ResultFlag_CursorNotFound;
        }
        else {
            // Quote: check for spoofing of the ns such that it does not match the one originally
            // there for the cursor
            uassert(17011, "auth error", str::equals(ns, cc->ns().c_str()));
            *isCursorAuthorized = true;

            // Reset timeout timer on the cursor since the cursor is still in use.
            cc->setIdleTime(0);

            // TODO: fail point?

            // If the operation that spawned this cursor had a time limit set, apply leftover
            // time to this getmore.
            curop.setMaxTimeMicros(cc->getLeftoverMaxTimeMicros());
            killCurrentOp.checkForInterrupt(); // May trigger maxTimeAlwaysTimeOut fail point.

            // TODO:
            // curop.debug().query = BSONForQuery
            // curop.setQuery(curop.debug().query);

            // TODO: What is pass?
            if (0 == pass) { cc->updateSlaveLocation(curop); }

            if (cc->isAggCursor) {
                // Agg cursors handle their own locking internally.
                ctx.reset(); // unlocks
            }

            CollectionMetadataPtr collMetadata = cc->getCollMetadata();

            // If we're replaying the oplog, we save the last time that we read.
            OpTime slaveReadTill;

            // What number result are we starting at?  Used to fill out the reply.
            startingResult = cc->pos();

            // What gives us results.
            Runner* runner = cc->getRunner();
            const int queryOptions = cc->queryOptions();

            // Get results out of the runner.
            runner->restoreState();

            BSONObj obj;
            Runner::RunnerState state;
            while (Runner::RUNNER_ADVANCED == (state = runner->getNext(&obj, NULL))) {
                // Add result to output buffer.
                bb.appendBuf((void*)obj.objdata(), obj.objsize());

                // Count the result.
                ++numResults;

                // Possibly note slave's position in the oplog.
                if (queryOptions & QueryOption_OplogReplay) {
                    BSONElement e = obj["ts"];
                    if (Date == e.type() || Timestamp == e.type()) {
                        slaveReadTill = e._opTime();
                    }
                }

                if ((ntoreturn && numResults >= ntoreturn)
                    || bb.len() > MaxBytesToReturnToClientAtOnce) {
                    break;
                }
            }

            if (Runner::RUNNER_EOF == state && 0 == numResults
                && (queryOptions & QueryOption_CursorTailable)
                && (queryOptions & QueryOption_AwaitData) && (pass < 1000)) {
                // If the cursor is tailable we don't kill it if it's eof.  We let it try to get
                // data some # of times first.
                return 0;
            }

            bool saveClientCursor = false;

            if (Runner::RUNNER_DEAD == state || Runner::RUNNER_ERROR == state) {
                // If we're dead there's no way to get more results.
                saveClientCursor = false;
                // In the old system tailable capped cursors would be killed off at the
                // cursorid level.  If a tailable capped cursor is nuked the cursorid
                // would vanish.
                // 
                // In the new system they die and are cleaned up later (or time out).
                // So this is where we get to remove the cursorid.
                if (0 == numResults) {
                    resultFlags = ResultFlag_CursorNotFound;
                }
            }
            else if (Runner::RUNNER_EOF == state) {
                // EOF is also end of the line unless it's tailable.
                saveClientCursor = queryOptions & QueryOption_CursorTailable;
            }
            else {
                verify(Runner::RUNNER_ADVANCED == state);
                saveClientCursor = true;
            }

            if (!saveClientCursor) {
                ccPin.deleteUnderlying();
                // cc is now invalid, as is the runner
                cursorid = 0;
                cc = NULL;
                QLOG() << "getMore NOT saving client cursor, ended w/state "
                       << Runner::statestr(state)
                       << endl;
            }
            else {
                // Continue caching the ClientCursor.
                cc->incPos(numResults);
                runner->saveState();
                QLOG() << "getMore saving client cursor ended w/state "
                       << Runner::statestr(state)
                       << endl;

                // Possibly note slave's position in the oplog.
                if ((queryOptions & QueryOption_OplogReplay) && !slaveReadTill.isNull()) {
                    cc->slaveReadTill(slaveReadTill);
                }

                exhaust = (queryOptions & QueryOption_Exhaust);

                // If the getmore had a time limit, remaining time is "rolled over" back to the
                // cursor (for use by future getmore ops).
                cc->setLeftoverMaxTimeMicros( curop.getRemainingMaxTimeMicros() );
            }
        }

        QueryResult* qr = reinterpret_cast<QueryResult*>(bb.buf());
        qr->len = bb.len();
        qr->setOperation(opReply);
        qr->_resultFlags() = resultFlags;
        qr->cursorId = cursorid;
        qr->startingFrom = startingResult;
        qr->nReturned = numResults;
        bb.decouple();
        QLOG() << "getMore returned " << numResults << " results\n";
        return qr;
    }

    Status getOplogStartHack(CanonicalQuery* cq, Runner** runnerOut) {
        // Make an oplog start finding stage.
        WorkingSet* oplogws = new WorkingSet();
        OplogStart* stage = new OplogStart(cq->ns(), cq->root(), oplogws);

        // Takes ownership of ws and stage.
        auto_ptr<InternalRunner> runner(new InternalRunner(cq->ns(), stage, oplogws));
        runner->setYieldPolicy(Runner::YIELD_AUTO);

        // The stage returns a DiskLoc of where to start.
        DiskLoc startLoc;
        Runner::RunnerState state = runner->getNext(NULL, &startLoc);

        // This is normal.  The start of the oplog is the beginning of the collection.
        if (Runner::RUNNER_EOF == state) { return getRunner(cq, runnerOut); }

        // This is not normal.  An error was encountered.
        if (Runner::RUNNER_ADVANCED != state) {
            return Status(ErrorCodes::InternalError,
                          "quick oplog start location had error...?");
        }

        // cout << "diskloc is " << startLoc.toString() << endl;

        // Build our collection scan...
        CollectionScanParams params;
        params.ns = cq->ns();
        params.start = startLoc;
        params.direction = CollectionScanParams::FORWARD;
        params.tailable = cq->getParsed().hasOption(QueryOption_CursorTailable);

        WorkingSet* ws = new WorkingSet();
        CollectionScan* cs = new CollectionScan(params, ws, cq->root());
        // Takes ownership of cq, cs, ws.
        *runnerOut = new SingleSolutionRunner(cq, NULL, cs, ws);
        return Status::OK();
    }

    std::string newRunQuery(Message& m, QueryMessage& q, CurOp& curop, Message &result) {
        // Validate the namespace.
        const char *ns = q.ns;
        uassert(16332, "can't have an empty ns", ns[0]);

        const NamespaceString nsString(ns);
        uassert(16256, str::stream() << "Invalid ns [" << ns << "]", nsString.isValid());

        // Set curop information.
        curop.debug().ns = ns;
        curop.debug().ntoreturn = q.ntoreturn;
        curop.debug().query = q.query;
        curop.setQuery(q.query);

        // If the query is really a command, run it.
        if (nsString.isCommand()) {
            int nToReturn = q.ntoreturn;
            uassert(16979, str::stream() << "bad numberToReturn (" << nToReturn
                                         << ") for $cmd type ns - can only be 1 or -1",
                    nToReturn == 1 || nToReturn == -1);

            curop.markCommand();

            BufBuilder bb;
            bb.skip(sizeof(QueryResult));

            BSONObjBuilder cmdResBuf;
            if (!runCommands(ns, q.query, curop, bb, cmdResBuf, false, q.queryOptions)) {
                uasserted(13530, "bad or malformed command request?");
            }

            curop.debug().iscommand = true;
            // TODO: Does this get overwritten/do we really need to set this twice?
            curop.debug().query = q.query;

            QueryResult* qr = reinterpret_cast<QueryResult*>(bb.buf());
            bb.decouple();
            qr->setResultFlagsToOk();
            qr->len = bb.len();
            curop.debug().responseLength = bb.len();
            qr->setOperation(opReply);
            qr->cursorId = 0;
            qr->startingFrom = 0;
            qr->nReturned = 1;
            result.setData(qr, true);
            return "";
        }

        // This is a read lock.  We require this because if we're parsing a $where, the
        // where-specific parsing code assumes we have a lock and creates execution machinery that
        // requires it.
        Client::ReadContext ctx(q.ns);

        // Parse the qm into a CanonicalQuery.
        CanonicalQuery* cq;
        Status canonStatus = CanonicalQuery::canonicalize(q, &cq);
        if (!canonStatus.isOK()) {
            uasserted(17287, str::stream() << "Can't canonicalize query: " << canonStatus.toString());
        }
        verify(cq);

        QLOG() << "Running query on new system: " << cq->toString();

        // Parse, canonicalize, plan, transcribe, and get a runner.
        Runner* rawRunner = NULL;

        // We use this a lot below.
        const LiteParsedQuery& pq = cq->getParsed();

        // We'll now try to get the query runner that will execute this query for us. There
        // are a few cases in which we know upfront which runner we should get and, therefore,
        // we shortcut the selection process here.
        //
        // (a) If the query is over a collection that doesn't exist, we get a special runner
        // that's is so (a runner) which doesn't return results, the EOFRunner.
        //
        // (b) if the query is a replication's initial sync one, we get a SingleSolutinRunner
        // that uses a specifically designed stage that skips extents faster (see details in
        // exec/oplogstart.h)
        //
        // Otherwise we go through the selection of which runner is most suited to the
        // query + run-time context at hand.
        Status status = Status::OK();
        if (ctx.ctx().db()->getCollection(cq->ns()) == NULL) {
            rawRunner = new EOFRunner(cq, cq->ns());
        }
        else if (pq.hasOption(QueryOption_OplogReplay)) {
            status = getOplogStartHack(cq, &rawRunner);
        }
        else {
            // Takes ownership of cq.
            size_t options = QueryPlannerParams::DEFAULT;
            if (shardingState.needCollectionMetadata(pq.ns())) {
                options |= QueryPlannerParams::INCLUDE_SHARD_FILTER;
            }
            status = getRunner(cq, &rawRunner, options);
        }

        if (!status.isOK()) {
            // NOTE: Do not access cq as getRunner has deleted it.
            uasserted(17007, "Unable to execute query: " + status.reason());
        }

        verify(NULL != rawRunner);
        auto_ptr<Runner> runner(rawRunner);

        // We freak out later if this changes before we're done with the query.
        const ChunkVersion shardingVersionAtStart = shardingState.getVersion(cq->ns());

        // Handle query option $maxTimeMS (not used with commands).
        curop.setMaxTimeMicros(static_cast<unsigned long long>(pq.getMaxTimeMS()) * 1000);
        killCurrentOp.checkForInterrupt(); // May trigger maxTimeAlwaysTimeOut fail point.

        // uassert if we are not on a primary, and not a secondary with SlaveOk query parameter set.
        replVerifyReadsOk(&pq);

        // If this exists, the collection is sharded.
        // If it doesn't exist, we can assume we're not sharded.
        // If we're sharded, we might encounter data that is not consistent with our sharding state.
        // We must ignore this data.
        CollectionMetadataPtr collMetadata;
        if (!shardingState.needCollectionMetadata(pq.ns())) {
            collMetadata = CollectionMetadataPtr();
        }
        else {
            collMetadata = shardingState.getCollectionMetadata(pq.ns());
        }

        // Run the query.
        // bb is used to hold query results
        // this buffer should contain either requested documents per query or
        // explain information, but not both
        BufBuilder bb(32768);
        bb.skip(sizeof(QueryResult));

        // How many results have we obtained from the runner?
        int numResults = 0;

        // If we're replaying the oplog, we save the last time that we read.
        OpTime slaveReadTill;

        // Do we save the Runner in a ClientCursor for getMore calls later?
        bool saveClientCursor = false;

        // We turn on auto-yielding for the runner here.  The runner registers itself with the
        // active runners list in ClientCursor.
        auto_ptr<ScopedRunnerRegistration> safety(new ScopedRunnerRegistration(runner.get()));
        runner->setYieldPolicy(Runner::YIELD_AUTO);

        BSONObj obj;
        Runner::RunnerState state;
        // uint64_t numMisplacedDocs = 0;

        // set this outside loop. we will need to use this both within loop and when deciding
        // to fill in explain information
        const bool isExplain = pq.isExplain();

        while (Runner::RUNNER_ADVANCED == (state = runner->getNext(&obj, NULL))) {
            // Add result to output buffer. This is unnecessary if explain info is requested
            if (!isExplain) {
                bb.appendBuf((void*)obj.objdata(), obj.objsize());
            }

            // Count the result.
            ++numResults;

            // Possibly note slave's position in the oplog.
            if (pq.hasOption(QueryOption_OplogReplay)) {
                BSONElement e = obj["ts"];
                if (Date == e.type() || Timestamp == e.type()) {
                    slaveReadTill = e._opTime();
                }
            }

            // TODO: only one type of 2d search doesn't support this.  We need a way to pull it out
            // of CanonicalQuery. :(
            const bool supportsGetMore = true;
            if (isExplain) {
                if (enoughForExplain(pq, numResults)) {
                    break;
                }
            }
            else if (!supportsGetMore && (enough(pq, numResults)
                                          || bb.len() >= MaxBytesToReturnToClientAtOnce)) {
                break;
            }
            else if (enoughForFirstBatch(pq, numResults, bb.len())) {
                QLOG() << "Enough for first batch, wantMore=" << pq.wantMore()
                       << " numToReturn=" << pq.getNumToReturn()
                       << " numResults=" << numResults
                       << endl;
                // If only one result requested assume it's a findOne() and don't save the cursor.
                if (pq.wantMore() && 1 != pq.getNumToReturn()) {
                    QLOG() << " runner EOF=" << runner->isEOF() << endl;
                    saveClientCursor = !runner->isEOF();
                }
                break;
            }
        }

        // If we cache the runner later, we want to deregister it as it receives notifications
        // anyway by virtue of being cached.
        //
        // If we don't cache the runner later, we are deleting it, so it must be deregistered.
        //
        // So, no matter what, deregister the runner.
        safety.reset();

        // Caller expects exceptions thrown in certain cases:
        // * in-memory sort using too much RAM.
        if (Runner::RUNNER_ERROR == state) {
            uasserted(17144, "Runner error, memory limit for sort probably exceeded");
        }

        // Why save a dead runner?
        if (Runner::RUNNER_DEAD == state) {
            saveClientCursor = false;
        }
        else if (pq.hasOption(QueryOption_CursorTailable)) {
            // If we're tailing a capped collection, we don't bother saving the cursor if the
            // collection is empty. Otherwise, the semantics of the tailable cursor is that the
            // client will keep trying to read from it. So we'll keep it around.
            Collection* collection = ctx.ctx().db()->getCollection(cq->ns());
            if (collection && collection->numRecords() != 0 && pq.getNumToReturn() != 1) {
                saveClientCursor = true;
            }
        }

        // TODO(greg): This will go away soon.
        if (!shardingState.getVersion(pq.ns()).isWriteCompatibleWith(shardingVersionAtStart)) {
            // if the version changed during the query we might be missing some data and its safe to
            // send this as mongos can resend at this point
            throw SendStaleConfigException(pq.ns(), "version changed during initial query",
                                           shardingVersionAtStart,
                                           shardingState.getVersion(pq.ns()));
        }

        // Append explain information to query results by asking the runner to produce them.
        if (isExplain) {
            TypeExplain* bareExplain;
            Status res = runner->getExplainPlan(&bareExplain);

            if (!res.isOK()) {
                error() << "could not produce explain of query '" << pq.getFilter()
                        << "', error: " << res.reason();
                // If numResults and the data in bb don't correspond, we'll crash later when rooting
                // through the reply msg.
                BSONObj emptyObj;
                bb.appendBuf((void*)emptyObj.objdata(), emptyObj.objsize());
                // The explain output is actually a result.
                numResults = 1;
                // TODO: we can fill out millis etc. here just fine even if the plan screwed up.
            }
            else {
                boost::scoped_ptr<TypeExplain> explain(bareExplain);

                // Fill in the missing run-time fields in explain, starting with propeties of
                // the process running the query.
                std::string server = mongoutils::str::stream()
                    << getHostNameCached() << ":" << serverGlobalParams.port;
                explain->setServer(server);

                // We might have skipped some results due to chunk migration etc. so our count is
                // correct.
                explain->setN(numResults);

                // Clock the whole operation.
                explain->setMillis(curop.elapsedMillis());

                BSONObj explainObj = explain->toBSON();
                bb.appendBuf((void*)explainObj.objdata(), explainObj.objsize());

                // The explain output is actually a result.
                numResults = 1;
            }
        }

        long long ccId = 0;
        if (saveClientCursor) {
            // We won't use the runner until it's getMore'd.
            runner->saveState();

            // Allocate a new ClientCursor.  We don't have to worry about leaking it as it's
            // inserted into a global map by its ctor.
            ClientCursor* cc = new ClientCursor(runner.get(), cq->getParsed().getOptions(),
                                                cq->getParsed().getFilter());
            ccId = cc->cursorid();

            QLOG() << "caching runner with cursorid " << ccId
                   << " after returning " << numResults << " results" << endl;

            // ClientCursor takes ownership of runner.  Release to make sure it's not deleted.
            runner.release();

            // TODO document
            if (pq.hasOption(QueryOption_OplogReplay) && !slaveReadTill.isNull()) {
                cc->slaveReadTill(slaveReadTill);
            }

            // TODO document
            if (pq.hasOption(QueryOption_Exhaust)) {
                curop.debug().exhaust = true;
            }

            // Set attributes for getMore.
            cc->setCollMetadata(collMetadata);
            cc->setPos(numResults);

            // If the query had a time limit, remaining time is "rolled over" to the cursor (for
            // use by future getmore ops).
            cc->setLeftoverMaxTimeMicros(curop.getRemainingMaxTimeMicros());
        }
        else {
            QLOG() << "not caching runner but returning " << numResults << " results\n";
        }

        // Add the results from the query into the output buffer.
        result.appendData(bb.buf(), bb.len());
        bb.decouple();

        // Fill out the output buffer's header.
        QueryResult* qr = static_cast<QueryResult*>(result.header());
        qr->cursorId = ccId;
        curop.debug().cursorid = (0 == ccId ? -1 : ccId);
        qr->setResultFlagsToOk();
        qr->setOperation(opReply);
        qr->startingFrom = 0;
        qr->nReturned = numResults;

        curop.debug().ntoskip = pq.getSkip();
        curop.debug().nreturned = numResults;

        // curop.debug().exhaust is set above.
        return curop.debug().exhaust ? pq.ns() : "";
    }

}  // namespace mongo
