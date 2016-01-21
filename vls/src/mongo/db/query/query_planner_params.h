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

#pragma once

#include <vector>

#include "mongo/db/jsobj.h"
#include "mongo/db/query/index_entry.h"

namespace mongo {

    struct QueryPlannerParams {
        QueryPlannerParams() : options(DEFAULT) { }

        enum Options {
            // You probably want to set this.
            DEFAULT = 0,

            // Set this if you don't want a table scan.
            // See http://docs.mongodb.org/manual/reference/parameters/
            NO_TABLE_SCAN = 1,

            // Set this if you want a collscan outputted even if there's an ixscan.
            INCLUDE_COLLSCAN = 1 << 1,

            // Set this if you're running on a sharded cluster.  We'll add a "drop all docs that
            // shouldn't be on this shard" stage before projection.
            //
            // In order to set this, you must check
            // shardingState.needCollectionMetadata(current_namespace) in the same lock that you use
            // to build the query runner. You must also wrap the Runner in a ClientCursor within the
            // same lock. See the comment on ShardFilterStage for details.
            INCLUDE_SHARD_FILTER = 1 << 2,

            // Set this if you don't want any plans with a blocking sort stage.  All sorts must be
            // provided by an index.
            NO_BLOCKING_SORT = 1 << 3,

            // Set this if you want to turn on index intersection.
            INDEX_INTERSECTION = 1 << 4,
        };

        // See Options enum above.
        size_t options;

        // What indices are available for planning?
        vector<IndexEntry> indices;

        // What's our shard key?  If INCLUDE_SHARD_FILTER is set we will create a shard filtering
        // stage.  If we know the shard key, we can perform covering analysis instead of always
        // forcing a fetch.
        BSONObj shardKey;
    };

}  // namespace mongo
