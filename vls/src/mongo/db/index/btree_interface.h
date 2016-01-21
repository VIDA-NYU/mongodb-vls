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

#include "mongo/db/diskloc.h"
#include "mongo/db/jsobj.h"
#include "mongo/db/structure/collection.h"

namespace mongo {

    class BtreeInMemoryState;

    /**
     * We have two Btree on-disk formats which support identical operations.  We hide this as much
     * as possible by having one implementation of this interface per format.
     *
     * For documentation on all of the methods here, look at mongo/db/structure/btree/btree.h
     */
    class BtreeInterface {
    public:
        virtual ~BtreeInterface() { }

        static BtreeInterface *interfaces[];

        // This is the # of the exception that is thrown if we're trying to access a bucket that
        // was deleted.  Calling code needs to be able to recognize this and possibly ignore it.
        static const int deletedBucketCode = 16738;

        virtual int bt_insert(BtreeInMemoryState* btreeState,
                              const DiskLoc thisLoc,
                              const DiskLoc recordLoc,
                              const BSONObj& key,
                              bool dupsallowed,
                              bool toplevel = true) = 0;

        virtual bool unindex(BtreeInMemoryState* btreeState,
                             const DiskLoc thisLoc,
                             const BSONObj& key,
                             const DiskLoc recordLoc) = 0;

        virtual DiskLoc locate(const BtreeInMemoryState* btreeState,
                               const DiskLoc& thisLoc,
                               const BSONObj& key,
                               int& pos, // out
                               bool& found, // out
                               const DiskLoc& recordLoc, // out
                               int direction = 1) const = 0;

        virtual bool wouldCreateDup(const BtreeInMemoryState* btreeState,
                                    const DiskLoc& thisLoc,
                                    const BSONObj& key,
                                    const DiskLoc& self) const = 0;

        virtual void customLocate(const BtreeInMemoryState* btreeState,
                                  DiskLoc& locInOut,
                                  int& keyOfs,
                                  const BSONObj& keyBegin,
                                  int keyBeginLen, bool afterKey,
                                  const vector<const BSONElement*>& keyEnd,
                                  const vector<bool>& keyEndInclusive,
                                  int direction,
                                  pair<DiskLoc, int>& bestParent) const = 0 ;

        virtual void advanceTo(const BtreeInMemoryState* btreeState,
                               DiskLoc &thisLoc,
                               int &keyOfs,
                               const BSONObj &keyBegin,
                               int keyBeginLen,
                               bool afterKey,
                               const vector<const BSONElement*>& keyEnd,
                               const vector<bool>& keyEndInclusive,
                               int direction) const = 0;

        virtual string dupKeyError(const BtreeInMemoryState* btreeState,
                                   DiskLoc bucket,
                                   const BSONObj& keyObj) const =0;

        virtual DiskLoc advance(const BtreeInMemoryState* btreeState,
                                const DiskLoc& thisLoc,
                                int& keyOfs,
                                int direction,
                                const char* caller) const = 0;
        
        virtual bool chronosNextObj(ID id, 
                                    uint64_t &scanStableMask,
                                    uint64_t &scanMask,
                                    st_mask_map* statusMaskMap,
                                    scan_set* scanSet) const = 0;
        
        virtual DiskLoc chronosAdvance(const BtreeInMemoryState* btreeState,
                                       const DiskLoc& thisLoc,
                                       DiskLoc& chronosLoc,
                                       DiskLoc& nextLoc,
                                       st_mask_map_vector* statusMaskMapVector,
                                       ofs_map* ofsMap,
                                       scan_set_vector* scanSetVector,
                                       uint64_t &scanStableMask,
                                       uint64_t &scanMask,
                                       IndexCatalog* indexCatalog,
                                       int idxNumber,
                                       unsigned int* entriesAltered,
                                       int& keyOfs,
                                       int direction,
                                       const char* caller) const = 0;
        
        virtual bool chronosVerify(const BtreeInMemoryState* btreeState,
                                   const DiskLoc& thisLoc,
                                   DiskLoc& nextLoc,
                                   st_mask_map_vector* statusMaskMapVector,
                                   ofs_map* ofsMap,
                                   scan_set_vector* scanSetVector,
                                   uint64_t &scanStableMask,
                                   uint64_t &scanMask,
                                   IndexCatalog* indexCatalog,
                                   int idxNumber,
                                   unsigned int* entriesAltered,
                                   int& keyOfs) const = 0;

        virtual long long fullValidate(const BtreeInMemoryState* btreeState,
                                       const DiskLoc& thisLoc,
                                       const BSONObj& keyPattern) = 0;

        /**
         * These methods are here so that the BtreeCursor doesn't need to do any templating for the
         * two on-disk formats.
         */

        /**
         * Is the key at (bucket, keyOffset) being used or not?
         * Some keys are marked as not used and skipped.
         */
        virtual bool keyIsUsed(DiskLoc bucket, int keyOffset) const = 0;

        /**
         * Get the BSON representation of the key at (bucket, keyOffset).
         */
        virtual BSONObj keyAt(DiskLoc bucket, int keyOffset) const = 0;

        /**
         * Get the DiskLoc that the key at (bucket, keyOffset) points at.
         */
        virtual DiskLoc recordAt(DiskLoc bucket, int keyOffset) const = 0;

        /**
         * keyAt and recordAt at the same time.
         */
        virtual void keyAndRecordAt(DiskLoc bucket, int keyOffset, BSONObj* keyOut,
                                    DiskLoc* recordOut) const = 0;
    };

}  // namespace mongo
