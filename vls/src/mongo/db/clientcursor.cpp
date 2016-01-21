/**
 *    Copyright (C) 2008, 2013 10gen Inc.
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

#include "mongo/pch.h"

#include "mongo/db/clientcursor.h"

#include <string>
#include <time.h>
#include <vector>

#include "mongo/client/dbclientinterface.h"
#include "mongo/db/audit.h"
#include "mongo/db/auth/action_set.h"
#include "mongo/db/auth/action_type.h"
#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/auth/privilege.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/server_status.h"
#include "mongo/db/db.h"
#include "mongo/db/introspect.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/kill_current_op.h"
#include "mongo/db/pagefault.h"
#include "mongo/db/repl/rs.h"
#include "mongo/db/repl/write_concern.h"
#include "mongo/platform/random.h"
#include "mongo/util/processinfo.h"
#include "mongo/util/timer.h"

namespace mongo {

    ClientCursor::CCById ClientCursor::clientCursorsById;
    boost::recursive_mutex& ClientCursor::ccmutex( *(new boost::recursive_mutex()) );
    long long ClientCursor::numberTimedOut = 0;
    ClientCursor::RunnerSet ClientCursor::nonCachedRunners;

    void aboutToDeleteForSharding(const StringData& ns,
                                  const Database* db,
                                  const NamespaceDetails* nsd,
                                  const DiskLoc& dl ); // from s/d_logic.h

    ClientCursor::ClientCursor(Runner* runner, int qopts, const BSONObj query) {
        _runner.reset(runner);
        _ns = runner->ns();
        _query = query;
        _queryOptions = qopts;
        init();
    }

    ClientCursor::ClientCursor(const string& ns)
        : _ns(ns),
          _queryOptions(QueryOption_NoCursorTimeout) {

        init();
    }

    void ClientCursor::init() {
        _db = cc().database();
        verify( _db );
        verify( _db->ownsNS( _ns ) );

        isAggCursor = false;

        _idleAgeMillis = 0;
        _leftoverMaxTimeMicros = 0;
        _pinValue = 0;
        _pos = 0;
        
        Lock::assertAtLeastReadLocked(_ns);

        if (_queryOptions & QueryOption_NoCursorTimeout) {
            // cursors normally timeout after an inactivity period to prevent excess memory use
            // setting this prevents timeout of the cursor in question.
            ++_pinValue;
        }

        recursive_scoped_lock lock(ccmutex);
        _cursorid = allocCursorId_inlock();
        clientCursorsById.insert( make_pair(_cursorid, this) );
    }

    ClientCursor::~ClientCursor() {
        if( _pos == -2 ) {
            // defensive: destructor called twice
            wassert(false);
            return;
        }

        {
            recursive_scoped_lock lock(ccmutex);
            clientCursorsById.erase(_cursorid);

            // defensive:
            _cursorid = INVALID_CURSOR_ID;
            _pos = -2;
            _pinValue = 0;
        }
    }

    // static
    void ClientCursor::assertNoCursors() {
        recursive_scoped_lock lock(ccmutex);
        if (clientCursorsById.size() > 0) {
            log() << "ERROR clientcursors exist but should not at this point" << endl;
            ClientCursor *cc = clientCursorsById.begin()->second;
            log() << "first one: " << cc->_cursorid << ' ' << cc->_ns << endl;
            clientCursorsById.clear();
            verify(false);
        }
    }

    void ClientCursor::invalidate(const StringData& ns) {
        Lock::assertWriteLocked(ns);

        size_t dot = ns.find( '.' );
        verify( dot != string::npos );

        // first (and only) dot is the last char
        bool isDB = dot == ns.size() - 1;

        Database *db = cc().database();
        verify(db);
        verify(ns.startsWith(db->name()));

        recursive_scoped_lock cclock(ccmutex);
        // Look at all active non-cached Runners.  These are the runners that are in auto-yield mode
        // that are not attached to the the client cursor. For example, all internal runners don't
        // need to be cached -- there will be no getMore.
        for (RunnerSet::iterator it = nonCachedRunners.begin(); it != nonCachedRunners.end();
             ++it) {

            Runner* runner = *it;
            const string& runnerNS = runner->ns();
            if ( ( isDB && StringData(runnerNS).startsWith(ns) ) || ns == runnerNS ) {
                runner->kill();
            }
        }

        // Look at all cached ClientCursor(s).  The CC may have a Runner, a Cursor, or nothing (see
        // sharding_block.h).
        CCById::const_iterator it = clientCursorsById.begin();
        while (it != clientCursorsById.end()) {
            ClientCursor* cc = it->second;

            // Aggregation cursors don't have their lifetime bound to the underlying collection.
            if (cc->isAggCursor) {
                ++it;
                continue;
            }

            // We're only interested in cursors over one db.
            if (cc->_db != db) {
                ++it;
                continue;
            }

            // Note that a valid ClientCursor state is "no cursor no runner."  This is because
            // the set of active cursor IDs in ClientCursor is used as representation of query
            // state.  See sharding_block.h.  TODO(greg,hk): Move this out.
            if (NULL == cc->_runner.get()) {
                ++it;
                continue;
            }

            bool shouldDelete = false;

            // We will only delete CCs with runners that are not actively in use.  The runners that
            // are actively in use are instead kill()-ed.
            if (NULL != cc->_runner.get()) {
                if (isDB || cc->_runner->ns() == ns) {
                    // If there is a pinValue >= 100, somebody is actively using the CC and we do
                    // not delete it.  Instead we notify the holder that we killed it.  The holder
                    // will then delete the CC.
                    if (cc->_pinValue >= 100) {
                        cc->_runner->kill();
                    }
                    else {
                        // pinvalue is <100, so there is nobody actively holding the CC.  We can
                        // safely delete it as nobody is holding the CC.
                        shouldDelete = true;
                    }
                }
            }

            if (shouldDelete) {
                ClientCursor* toDelete = it->second;
                CursorId id = toDelete->cursorid();
                delete toDelete;
                // We're not following the usual paradigm of saving it, ++it, and deleting the saved
                // 'it' because deleting 'it' might invalidate the next thing in clientCursorsById.
                // TODO: Why?
                it = clientCursorsById.upper_bound(id);
            }
            else {
                ++it;
            }
        }
    }

    /* must call this on a delete so we clean up the cursors. */
    void ClientCursor::aboutToDelete(const StringData& ns,
                                     const NamespaceDetails* nsd,
                                     const DiskLoc& dl) {
        // Begin cursor-only
        NoPageFaultsAllowed npfa;
        // End cursor-only

        recursive_scoped_lock lock(ccmutex);

        Database *db = cc().database();
        verify(db);

        aboutToDeleteForSharding( ns, db, nsd, dl );

        // Check our non-cached active runner list.
        for (RunnerSet::iterator it = nonCachedRunners.begin(); it != nonCachedRunners.end();
             ++it) {

            Runner* runner = *it;
            if (0 == ns.compare(runner->ns())) {
                runner->invalidate(dl);
            }
        }

        // TODO: This requires optimization.  We walk through *all* CCs and send the delete to every
        // CC open on the db we're deleting from.  We could:
        // 1. Map from ns to open runners,
        // 2. Map from ns -> (a map of DiskLoc -> runners who care about that DL)
        //
        // We could also queue invalidations somehow and have them processed later in the runner's
        // read locks.
        for (CCById::const_iterator it = clientCursorsById.begin(); it != clientCursorsById.end();
             ++it) {

            ClientCursor* cc = it->second;
            // We're only interested in cursors over one db.
            if (cc->_db != db) { continue; }
            if (NULL == cc->_runner.get()) { continue; }
            cc->_runner->invalidate(dl);
        }
    }

    void ClientCursor::registerRunner(Runner* runner) {
        recursive_scoped_lock lock(ccmutex);
        // The second part of the pair returned by unordered_map::insert tells us whether the
        // insert was performed or not, so we can use that to ensure that we have not been
        // double registered.
        const std::pair<RunnerSet::iterator, bool> result = nonCachedRunners.insert(runner);
        verify(result.second);
    }

    void ClientCursor::deregisterRunner(Runner* runner) {
        recursive_scoped_lock lock(ccmutex);
        // unordered_set::erase returns a count of how many elements were erased, so we can
        // validate that our de-registration matched an existing register call by ensuring that
        // exactly one item was erased.
        verify(nonCachedRunners.erase(runner) == 1);
    }

    void yieldOrSleepFor1Microsecond() {
#ifdef _WIN32
        SwitchToThread();
#elif defined(__linux__)
        pthread_yield();
#else
        sleepmicros(1);
#endif
    }

    void ClientCursor::staticYield(int micros, const StringData& ns, const Record* rec) {
        bool haveReadLock = Lock::isReadLocked();

        killCurrentOp.checkForInterrupt( false );
        {
            auto_ptr<LockMongoFilesShared> lk;
            if ( rec ) {
                // need to lock this else rec->touch won't be safe file could disappear
                lk.reset( new LockMongoFilesShared() );
            }

            dbtempreleasecond unlock;
            if ( unlock.unlocked() ) {
                if ( haveReadLock ) {
                    // This sleep helps reader threads yield to writer threads.
                    // Without this, the underlying reader/writer lock implementations
                    // are not sufficiently writer-greedy.
#ifdef _WIN32
                    SwitchToThread();
#else
                    if ( micros == 0 ) {
                        yieldOrSleepFor1Microsecond();
                    }
                    else {
                        sleepmicros(1);
                    }
#endif
                }
                else {
                    if ( micros == -1 ) {
                        sleepmicros(Client::recommendedYieldMicros());
                    }
                    else if ( micros == 0 ) {
                        yieldOrSleepFor1Microsecond();
                    }
                    else if ( micros > 0 ) {
                        sleepmicros( micros );
                    }
                }

            }
            else if ( Listener::getTimeTracker() == 0 ) {
                // we aren't running a server, so likely a repair, so don't complain
            }
            else {
                CurOp * c = cc().curop();
                while ( c->parent() )
                    c = c->parent();
                warning() << "ClientCursor::yield can't unlock b/c of recursive lock"
                          << " ns: " << ns 
                          << " top: " << c->info()
                          << endl;
            }

            if ( rec )
                rec->touch();

            lk.reset(0); // need to release this before dbtempreleasecond
        }
    }

    //
    // Timing and timeouts
    //

    bool ClientCursor::shouldTimeout(unsigned millis) {
        _idleAgeMillis += millis;
        return _idleAgeMillis > 600000 && _pinValue == 0;
    }

    void ClientCursor::setIdleTime( unsigned millis ) {
        _idleAgeMillis = millis;
    }

    void ClientCursor::idleTimeReport(unsigned millis) {
        bool foundSomeToTimeout = false;

        // two passes so that we don't need to readlock unless we really do some timeouts
        // we assume here that incrementing _idleAgeMillis outside readlock is ok.
        {
            recursive_scoped_lock lock(ccmutex);
            {
                unsigned sz = clientCursorsById.size();
                static time_t last;
                if( sz >= 100000 ) { 
                    if( time(0) - last > 300 ) {
                        last = time(0);
                        log() << "warning number of open cursors is very large: " << sz << endl;
                    }
                }
            }
            for ( CCById::iterator i = clientCursorsById.begin(); i != clientCursorsById.end();  ) {
                CCById::iterator j = i;
                i++;
                if( j->second->shouldTimeout( millis ) ) {
                    foundSomeToTimeout = true;
                }
            }
        }

        if( foundSomeToTimeout ) {
            Lock::GlobalRead lk;

            recursive_scoped_lock cclock(ccmutex);
            CCById::const_iterator it = clientCursorsById.begin();
            while (it != clientCursorsById.end()) {
                ClientCursor* cc = it->second;
                if( cc->shouldTimeout(0) ) {
                    numberTimedOut++;
                    LOG(1) << "killing old cursor " << cc->_cursorid << ' ' << cc->_ns
                           << " idle:" << cc->idleTime() << "ms\n";
                    ClientCursor* toDelete = it->second;
                    CursorId id = toDelete->cursorid();
                    // This is what winds up removing it from the map.
                    delete toDelete;
                    it = clientCursorsById.upper_bound(id);
                }
                else {
                    ++it;
                }
            }
        }
    }

    void ClientCursor::updateSlaveLocation( CurOp& curop ) {
        if ( _slaveReadTill.isNull() )
            return;
        mongo::updateSlaveLocation( curop , _ns.c_str() , _slaveReadTill );
    }

    void ClientCursor::appendStats( BSONObjBuilder& result ) {
        recursive_scoped_lock lock(ccmutex);
        result.appendNumber("totalOpen", clientCursorsById.size() );
        result.appendNumber("clientCursors_size", (int) numCursors());
        result.appendNumber("timedOut" , numberTimedOut);
        unsigned pinned = 0;
        unsigned notimeout = 0;
        for ( CCById::iterator i = clientCursorsById.begin(); i != clientCursorsById.end(); i++ ) {
            unsigned p = i->second->_pinValue;
            if( p >= 100 )
                pinned++;
            else if( p > 0 )
                notimeout++;
        }
        if( pinned ) 
            result.append("pinned", pinned);
        if( notimeout )
            result.append("totalNoTimeout", notimeout);
    }

    //
    // ClientCursor creation/deletion/access.
    //

    // Some statics used by allocCursorId_inlock().
    namespace {
        // so we don't have to do find() which is a little slow very often.
        long long cursorGenTSLast = 0;
        PseudoRandom* cursorGenRandom = NULL;
    }

    long long ClientCursor::allocCursorId_inlock() {
        // It is important that cursor IDs not be reused within a short period of time.
        if (!cursorGenRandom) {
            scoped_ptr<SecureRandom> sr( SecureRandom::create() );
            cursorGenRandom = new PseudoRandom( sr->nextInt64() );
        }

        const long long ts = Listener::getElapsedTimeMillis();
        long long x;
        while ( 1 ) {
            x = ts << 32;
            x |= cursorGenRandom->nextInt32();

            if ( x == 0 ) { continue; }

            if ( x < 0 ) { x *= -1; }

            if ( ts != cursorGenTSLast || ClientCursor::find_inlock(x, false) == 0 )
                break;
        }

        cursorGenTSLast = ts;
        return x;
    }

    // static
    ClientCursor* ClientCursor::find_inlock(CursorId id, bool warn) {
        CCById::iterator it = clientCursorsById.find(id);
        if ( it == clientCursorsById.end() ) {
            if ( warn ) {
                OCCASIONALLY out() << "ClientCursor::find(): cursor not found in map '" << id
                    << "' (ok after a drop)" << endl;
            }
            return 0;
        }
        return it->second;
    }

    void ClientCursor::find( const string& ns , set<CursorId>& all ) {
        recursive_scoped_lock lock(ccmutex);

        for ( CCById::iterator i=clientCursorsById.begin(); i!=clientCursorsById.end(); ++i ) {
            if ( i->second->_ns == ns )
                all.insert( i->first );
        }
    }

    // static
    ClientCursor* ClientCursor::find(CursorId id, bool warn) {
        recursive_scoped_lock lock(ccmutex);
        ClientCursor *c = find_inlock(id, warn);
        // if this asserts, your code was not thread safe - you either need to set no timeout
        // for the cursor or keep a ClientCursor::Pointer in scope for it.
        massert( 12521, "internal error: use of an unlocked ClientCursor", c == 0 || c->_pinValue );
        return c;
    }

    void ClientCursor::_erase_inlock(ClientCursor* cursor) {
        // Must not have an active ClientCursor::Pin.
        massert( 16089,
                str::stream() << "Cannot kill active cursor " << cursor->cursorid(),
                cursor->_pinValue < 100 );

        delete cursor;
    }

    bool ClientCursor::erase(CursorId id) {
        recursive_scoped_lock lock(ccmutex);
        ClientCursor* cursor = find_inlock(id);
        if (!cursor) { return false; }
        _erase_inlock(cursor);
        return true;
    }

    int ClientCursor::erase(int n, long long *ids) {
        int found = 0;
        for ( int i = 0; i < n; i++ ) {
            if ( erase(ids[i]))
                found++;

            if ( inShutdown() )
                break;
        }
        return found;
    }

    bool ClientCursor::eraseIfAuthorized(CursorId id) {
        NamespaceString ns;
        {
            recursive_scoped_lock lock(ccmutex);
            ClientCursor* cursor = find_inlock(id);
            if (!cursor) {
                audit::logKillCursorsAuthzCheck(
                        &cc(),
                        NamespaceString(),
                        id,
                        ErrorCodes::CursorNotFound);
                return false;
            }
            ns = NamespaceString(cursor->ns());
        }

        // Can't be in a lock when checking authorization
        const bool isAuthorized = cc().getAuthorizationSession()->isAuthorizedForActionsOnNamespace(
                ns, ActionType::killCursors);
        audit::logKillCursorsAuthzCheck(
                &cc(),
                ns,
                id,
                isAuthorized ? ErrorCodes::OK : ErrorCodes::Unauthorized);
        if (!isAuthorized) {
            return false;
        }

        // It is safe to lookup the cursor again after temporarily releasing the mutex because
        // of 2 invariants: that the cursor ID won't be re-used in a short period of time, and that
        // the namespace associated with a cursor cannot change.
        recursive_scoped_lock lock(ccmutex);
        ClientCursor* cursor = find_inlock(id);
        if (!cursor) {
            // Cursor was deleted in another thread since we found it earlier in this function.
            return false;
        }
        if (ns != cursor->ns()) {
            warning() << "Cursor namespace changed. Previous ns: " << ns << ", current ns: "
                    << cursor->ns() << endl;
            return false;
        }

        _erase_inlock(cursor);
        return true;
    }

    int ClientCursor::eraseIfAuthorized(int n, long long *ids) {
        int found = 0;
        for ( int i = 0; i < n; i++ ) {
            if ( eraseIfAuthorized(ids[i]))
                found++;

            if ( inShutdown() )
                break;
        }
        return found;
    }

    int ClientCursor::suggestYieldMicros() {
        int writers = 0;
        int readers = 0;

        int micros = Client::recommendedYieldMicros( &writers , &readers );

        if ( micros > 0 && writers == 0 && Lock::isR() ) {
            // we have a read lock, and only reads are coming on, so why bother unlocking
            return 0;
        }

        wassert( micros < 10000000 );
        dassert( micros <  1000001 );
        return micros;
    }

    //
    // Pin methods
    // TODO: Simplify when we kill Cursor.  In particular, once we've pinned a CC, it won't be
    // deleted from underneath us, so we can save the pointer and ignore the ID.
    //

    ClientCursorPin::ClientCursorPin(long long cursorid) : _cursorid( INVALID_CURSOR_ID ) {
        recursive_scoped_lock lock( ClientCursor::ccmutex );
        ClientCursor *cursor = ClientCursor::find_inlock( cursorid, true );
        if (NULL != cursor) {
            uassert( 12051, "clientcursor already in use? driver problem?",
                    cursor->_pinValue < 100 );
            cursor->_pinValue += 100;
            _cursorid = cursorid;
        }
    }

    ClientCursorPin::~ClientCursorPin() { DESTRUCTOR_GUARD( release(); ) }

    void ClientCursorPin::release() {
        if ( _cursorid == INVALID_CURSOR_ID ) {
            return;
        }
        ClientCursor *cursor = c();
        _cursorid = INVALID_CURSOR_ID;
        if ( cursor ) {
            verify( cursor->_pinValue >= 100 );
            cursor->_pinValue -= 100;
        }
    }

    void ClientCursorPin::deleteUnderlying() {
        if (_cursorid == INVALID_CURSOR_ID) {
            return;
        }
        ClientCursor *cursor = c();
        _cursorid = INVALID_CURSOR_ID;
        delete cursor;
    }

    ClientCursor* ClientCursorPin::c() const {
        return ClientCursor::find( _cursorid );
    }

    //
    // ClientCursorMonitor
    //

    // Used by sayMemoryStatus below.
    struct Mem { 
        Mem() { res = virt = mapped = 0; }
        long long res;
        long long virt;
        long long mapped;
        bool grew(const Mem& r) { 
            return (r.res && (((double)res)/r.res)>1.1 ) ||
              (r.virt && (((double)virt)/r.virt)>1.1 ) ||
              (r.mapped && (((double)mapped)/r.mapped)>1.1 );
        }
    };

    /**
     * called once a minute from killcursors thread
     */
    void sayMemoryStatus() { 
        static time_t last;
        static Mem mlast;
        try {
            ProcessInfo p;
            if (!serverGlobalParams.quiet && p.supported()) {
                Mem m;
                m.res = p.getResidentSize();
                m.virt = p.getVirtualMemorySize();
                m.mapped = MemoryMappedFile::totalMappedLength() / (1024 * 1024);
                time_t now = time(0);
                if( now - last >= 300 || m.grew(mlast) ) { 
                    log() << "mem (MB) res:" << m.res << " virt:" << m.virt;
                    long long totalMapped = m.mapped;
                    if (storageGlobalParams.dur) {
                        totalMapped *= 2;
                        log() << " mapped (incl journal view):" << totalMapped;
                    }
                    else {
                        log() << " mapped:" << totalMapped;
                    }
                    log() << " connections:" << Listener::globalTicketHolder.used();
                    if (theReplSet) {
                        log() << " replication threads:" << 
                            ReplSetImpl::replWriterThreadCount + 
                            ReplSetImpl::replPrefetcherThreadCount;
                    }
                    last = now;
                    mlast = m;
                }
            }
        }
        catch(const std::exception&) {
            log() << "ProcessInfo exception" << endl;
        }
    }

    void ClientCursorMonitor::run() {
        Client::initThread("clientcursormon");
        Client& client = cc();
        Timer t;
        const int Secs = 4;
        unsigned n = 0;
        while ( ! inShutdown() ) {
            ClientCursor::idleTimeReport( t.millisReset() );
            sleepsecs(Secs);
            if( ++n % (60/4) == 0 /*once a minute*/ ) { 
                sayMemoryStatus();
            }
        }
        client.shutdown();
    }

    ClientCursorMonitor clientCursorMonitor;

    //
    // cursorInfo command.
    //

    // QUESTION: Restrict to the namespace from which this command was issued?
    // Alternatively, make this command admin-only?
    class CmdCursorInfo : public Command {
    public:
        CmdCursorInfo() : Command( "cursorInfo", true ) {}
        virtual bool slaveOk() const { return true; }
        virtual void help( stringstream& help ) const {
            help << " example: { cursorInfo : 1 }";
        }
        virtual LockType locktype() const { return NONE; }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {
            ActionSet actions;
            actions.addAction(ActionType::cursorInfo);
            out->push_back(Privilege(ResourcePattern::forClusterResource(), actions));
        }
        bool run(const string& dbname, BSONObj& jsobj, int, string& errmsg, BSONObjBuilder& result,
                 bool fromRepl ) {
            ClientCursor::appendStats( result );
            return true;
        }
    } cmdCursorInfo;

    //
    // cursors stats.
    //

    class CursorServerStats : public ServerStatusSection {
    public:
        CursorServerStats() : ServerStatusSection( "cursors" ){}
        virtual bool includeByDefault() const { return true; }

        BSONObj generateSection(const BSONElement& configElement) const {
            BSONObjBuilder b;
            ClientCursor::appendStats( b );
            return b.obj();
        }
    } cursorServerStats;

} // namespace mongo
