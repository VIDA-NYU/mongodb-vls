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

#include <vector>

#include "mongo/base/status.h"
#include "mongo/db/diskloc.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/index/btree_interface.h"
#include "mongo/db/index/index_cursor.h"
#include "mongo/db/index/index_descriptor.h"
#include "mongo/db/structure/btree/state.h"

namespace mongo {

    class BtreeIndexCursor : public IndexCursor {
    public:
        virtual ~BtreeIndexCursor();

        bool isEOF() const;
        bool isInvalid() const;
        bool isChronosExec() const;

        // See nasty comment in .cpp
        virtual DiskLoc getBucket() const;
        virtual int getKeyOfs() const;

        /**
         * Called from btree.cpp when we're about to delete a Btree bucket.
         */
        static void aboutToDeleteBucket(const DiskLoc& bucket);

        virtual Status setOptions(const CursorOptions& options);

        virtual Status seek(const BSONObj& position);

        // Btree-specific seeking functions.
        Status seek(const vector<const BSONElement*>& position,
                    const vector<bool>& inclusive);

        Status skip(const BSONObj &keyBegin, int keyBeginLen, bool afterKey,
                    const vector<const BSONElement*>& keyEnd,
                    const vector<bool>& keyEndInclusive);

        virtual BSONObj getKey() const;
        virtual DiskLoc getValue() const;
        virtual void next();

        virtual Status savePosition();

        virtual Status restorePosition();

        virtual string toString();
        
        virtual void setChronosParameters(st_mask_map_vector* statusMaskMapVector,
                                          ofs_map* ofsMap,
                                          scan_set_vector* scanSetVector,
                                          IndexCatalog* indexCatalog,
                                          int idxNumber,
                                          unsigned int &entriesAltered);
        
        virtual DiskLoc getCurrentLoc() const;
        
        virtual void verifyDocument();

    private:
        // We keep the constructor private and only allow the AM to create us.
        friend class BtreeAccessMethod;

        // For handling bucket deletion.
        static unordered_set<BtreeIndexCursor*> _activeCursors;
        static SimpleMutex _activeCursorsMutex;

        // Go forward by default.
        BtreeIndexCursor(const BtreeInMemoryState* btreeState,
                         BtreeInterface *interface,
                         uint64_t &scanStableMask,
                         uint64_t &scanMask);

        void skipUnusedKeys();

        bool isSavedPositionValid();

        // Move to the next/prev. key.  Used by normal getNext and also skipping unused keys.
        void advance(const char* caller);

        // For saving/restoring position.
        BSONObj _savedKey;
        DiskLoc _savedLoc;

        BSONObj _emptyObj;

        int _direction;
        const BtreeInMemoryState* _btreeState; // not-owned
        BtreeInterface* _interface;

        // What are we looking at RIGHT NOW?  We look at a bucket.
        DiskLoc _bucket;
        // And we look at an offset in the bucket.
        int _keyOffset;
        
        // --------- VLS --------- //
        
        uint64_t &_scanStableMask;
        uint64_t &_scanMask;
        st_mask_map_vector* _statusMaskMapVector;
        ofs_map* _ofsMap;
        scan_set_vector* _scanSetVector;
        IndexCatalog* _indexCatalog;
        bool _use_chronos;
        int _idxNumber;
        unsigned int *_entriesAltered;
        
        DiskLoc _chronosBucket;
        DiskLoc _loc;
        
        // --------- VLS --------- //
    };

}  // namespace mongo
