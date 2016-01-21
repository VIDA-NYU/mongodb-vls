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

#pragma once

#include "mongo/base/status.h"
#include "mongo/db/index/2d_common.h"
#include "mongo/db/index/btree_access_method_internal.h"
#include "mongo/db/jsobj.h"

namespace mongo {

    class BtreeInMemoryState;
    class IndexCursor;
    class IndexDescriptor;
    struct TwoDIndexingParams;

    namespace twod_exec {
        class GeoPoint;
        class GeoAccumulator;
        class GeoBrowse;
        class GeoHopper;
        class GeoSearch;
        class GeoCircleBrowse;
        class GeoBoxBrowse;
        class GeoPolygonBrowse;
        class TwoDGeoNearRunner;
    }

    namespace twod_internal {
        class GeoPoint;
        class GeoAccumulator;
        class GeoBrowse;
        class GeoHopper;
        class GeoSearch;
        class GeoCircleBrowse;
        class GeoBoxBrowse;
        class GeoPolygonBrowse;
        class TwoDGeoNearRunner;
    }

    class TwoDAccessMethod : public BtreeBasedAccessMethod {
    public:
        using BtreeBasedAccessMethod::_descriptor;
        using BtreeBasedAccessMethod::_interface;

        TwoDAccessMethod(BtreeInMemoryState* btreeState);
        virtual ~TwoDAccessMethod() { }

        virtual Status newCursor(IndexCursor** out) const;
        virtual Status newCursor(IndexCursor **out, uint64_t &scanStableMask, uint64_t &scanMask) const;
        virtual void setCollection(Collection *collection);

    private:
        friend class TwoDIndexCursor;
        friend class twod_internal::GeoPoint;
        friend class twod_internal::GeoAccumulator;
        friend class twod_internal::GeoBrowse;
        friend class twod_internal::GeoHopper;
        friend class twod_internal::GeoSearch;
        friend class twod_internal::GeoCircleBrowse;
        friend class twod_internal::GeoBoxBrowse;
        friend class twod_internal::GeoPolygonBrowse;

        friend class twod_exec::GeoPoint;
        friend class twod_exec::GeoAccumulator;
        friend class twod_exec::GeoBrowse;
        friend class twod_exec::GeoHopper;
        friend class twod_exec::GeoSearch;
        friend class twod_exec::GeoCircleBrowse;
        friend class twod_exec::GeoBoxBrowse;
        friend class twod_exec::GeoPolygonBrowse;

        friend class twod_internal::TwoDGeoNearRunner;

        BtreeInterface* getInterface() { return _interface; }
        const IndexDescriptor* getDescriptor() { return _descriptor; }
        TwoDIndexingParams& getParams() { return _params; }

        // This really gets the 'locs' from the provided obj.
        void getKeys(const BSONObj& obj, vector<BSONObj>& locs) const;

        virtual void getKeys(const BSONObj& obj, BSONObjSet* keys);

        // This is called by the two getKeys above.
        void getKeys(const BSONObj &obj, BSONObjSet* keys, vector<BSONObj>* locs) const;
        
        virtual void addRemovedKey(const DiskLoc& loc, const BSONObj& key, const char* fieldName);

        BSONObj _nullObj;
        BSONElement _nullElt;
        TwoDIndexingParams _params;
    };

}  // namespace mongo
