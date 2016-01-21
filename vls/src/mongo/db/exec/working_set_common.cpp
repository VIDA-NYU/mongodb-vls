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

#include "mongo/db/exec/working_set.h"
#include "mongo/db/exec/working_set_common.h"
#include "mongo/db/pdfile.h"

namespace mongo {

    // static
    bool WorkingSetCommon::fetchAndInvalidateLoc(WorkingSetMember* member) {
        // Already in our desired state.
        if (member->state == WorkingSetMember::OWNED_OBJ) { return true; }

        // We can't do anything without a DiskLoc.
        if (!member->hasLoc()) { return false; }

        // Do the fetch, invalidate the DL.
        member->obj = member->loc.obj().getOwned();

        member->state = WorkingSetMember::OWNED_OBJ;
        member->loc = DiskLoc();
        return true;
    }

    // static
    void WorkingSetCommon::initFrom(WorkingSetMember* dest, const WorkingSetMember& src) {
        dest->loc = src.loc;
        dest->obj = src.obj;
        dest->keyData = src.keyData;
        dest->state = src.state;

        // Merge computed data.
        typedef WorkingSetComputedDataType WSCD;
        for (WSCD i = WSCD(0); i < WSM_COMPUTED_NUM_TYPES; i = WSCD(i + 1)) {
            if (src.hasComputed(i)) {
                dest->addComputed(src.getComputed(i)->clone());
            }
        }
    }

}  // namespace mongo
