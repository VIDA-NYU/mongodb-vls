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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#include "mongo/db/query/interval.h"

namespace mongo {

    namespace {

        /** Returns true if lhs and rhs intersection is not empty */
        bool intersects(const Interval& lhs, const Interval& rhs) {
            int res = lhs.start.woCompare(rhs.end, false);
            if (res > 0) {
                return false;
            }
            else if (res == 0 && (!lhs.startInclusive || !rhs.endInclusive)) {
                return false;
            }

            res = rhs.start.woCompare(lhs.end, false);
            if (res > 0) {
                return false;
            }
            else if (res == 0 && (!rhs.startInclusive || !lhs.endInclusive)) {
                return false;
            }

            return true;
        }

        /** Returns true if lhs and rhs represent the same interval */
        bool exact(const Interval& lhs, const Interval& rhs) {
            if (lhs.startInclusive != rhs.startInclusive) {
                return false;
            }

            if (lhs.endInclusive != rhs.endInclusive) {
                return false;
            }

            int res = lhs.start.woCompare(rhs.start, false);
            if (res != 0) {
                return false;
            }

            res = lhs.end.woCompare(rhs.end, false);
            if (res != 0) {
                return false;
            }

            return true;
        }

        /** Returns true if lhs is fully withing rhs */
        bool within(const Interval& lhs, const Interval& rhs) {
            int res = lhs.start.woCompare(rhs.start, false);
            if (res < 0) {
                return false;
            }
            else if (res == 0 && lhs.startInclusive && !rhs.startInclusive) {
                return false;
            }

            res = lhs.end.woCompare(rhs.end, false);
            if (res > 0) {
                return false;
            }
            else if (res == 0 && lhs.endInclusive && !rhs.endInclusive) {
                return false;
            }

            return true;
        }

        /** Returns true if the start of lhs comes before the start of rhs */
        bool precedes(const Interval& lhs, const Interval& rhs) {
            int res = lhs.start.woCompare(rhs.start, false);
            if (res < 0) {
                return true;
            }
            else if (res == 0 && lhs.startInclusive && !rhs.startInclusive) {
                return true;
            }
            return false;
        }

    } // unnamed namespace

    Interval::Interval()
        : _intervalData(BSONObj()), start(BSONElement()), startInclusive(false), end(BSONElement()),
          endInclusive(false) { }

    Interval::Interval(BSONObj base, bool si, bool ei) {
        init(base, si, ei);
    }

    void Interval::init(BSONObj base, bool si, bool ei) {
        verify(base.nFields() >= 2);

        _intervalData = base.getOwned();
        BSONObjIterator it(_intervalData);
        start = it.next();
        end = it.next();
        startInclusive = si;
        endInclusive = ei;
    }

    bool Interval::isEmpty() const {
        return _intervalData.nFields() == 0;
    }

    // TODO: shortcut number of comparisons
    Interval::IntervalComparison Interval::compare(const Interval& other) const {
        //
        // Intersect cases
        //

        // TODO: rewrite this to be member functions so semantics are clearer.
        if (intersects(*this, other)) {
            if (exact(*this, other)) {
                return INTERVAL_EQUALS;
            }
            if (within(*this, other)) {
                return INTERVAL_WITHIN;
            }
            if (within(other, *this)) {
                return INTERVAL_CONTAINS;
            }
            if (precedes(*this, other)) {
                return INTERVAL_OVERLAPS_BEFORE;
            }
            return INTERVAL_OVERLAPS_AFTER;
        }

        //
        // Non-intersect cases
        //

        if (precedes(*this, other)) {
            // It's not possible for both endInclusive and other.startInclusive to be true because
            // the bounds would intersect. Refer to section on "Intersect cases" above.
            if ((endInclusive || other.startInclusive) && 0 == end.woCompare(other.start, false)) {
                return INTERVAL_PRECEDES_COULD_UNION;
            }
            return INTERVAL_PRECEDES;
        }

        return INTERVAL_SUCCEEDS;
    }

    void Interval::intersect(const Interval& other, IntervalComparison cmp) {
        if (cmp == INTERVAL_UNKNOWN) {
            cmp = this->compare(other);
        }

        BSONObjBuilder builder;
        switch (cmp) {

        case INTERVAL_EQUALS:
        case INTERVAL_WITHIN:
            break;

        case INTERVAL_CONTAINS:
            builder.append(other.start);
            builder.append(other.end);
            init(builder.obj(), other.startInclusive, other.endInclusive);
            break;

        case INTERVAL_OVERLAPS_AFTER:
            builder.append(start);
            builder.append(other.end);
            init(builder.obj(), startInclusive, other.endInclusive);
            break;

        case INTERVAL_OVERLAPS_BEFORE:
            builder.append(other.start);
            builder.append(end);
            init(builder.obj(), other.startInclusive, endInclusive);
            break;

        case INTERVAL_PRECEDES:
        case INTERVAL_SUCCEEDS:
            *this = Interval();
            break;

        default:
            verify(false);
        }
    }

    void Interval::combine(const Interval& other, IntervalComparison cmp) {
        if (cmp == INTERVAL_UNKNOWN) {
            cmp = this->compare(other);
        }

        BSONObjBuilder builder;
        switch (cmp) {

        case INTERVAL_EQUALS:
        case INTERVAL_CONTAINS:
            break;

        case INTERVAL_WITHIN:
            builder.append(other.start);
            builder.append(other.end);
            init(builder.obj(), other.startInclusive, other.endInclusive);
            break;

        case INTERVAL_OVERLAPS_AFTER:
        case INTERVAL_SUCCEEDS:
            builder.append(other.start);
            builder.append(end);
            init(builder.obj(), other.startInclusive, endInclusive);
            break;

        case INTERVAL_OVERLAPS_BEFORE:
        case INTERVAL_PRECEDES:
            builder.append(start);
            builder.append(other.end);
            init(builder.obj(), startInclusive, other.endInclusive);
            break;

        default:
            verify(false);
        }
    }

    // static
    string Interval::cmpstr(IntervalComparison c) {
        if (c == INTERVAL_EQUALS) {
            return "INTERVAL_EQUALS";
        }

        // 'this' contains the other interval.
        if (c == INTERVAL_CONTAINS) {
            return "INTERVAL_CONTAINS";
        }

        // 'this' is contained by the other interval.
        if (c == INTERVAL_WITHIN) {
            return "INTERVAL_WITHIN";
        }

        // The two intervals intersect and 'this' is before the other interval.
        if (c == INTERVAL_OVERLAPS_BEFORE) {
            return "INTERVAL_OVERLAPS_BEFORE";
        }

        // The two intervals intersect and 'this is after the other interval.
        if (c == INTERVAL_OVERLAPS_AFTER) {
            return "INTERVAL_OVERLAPS_AFTER";
        }

        // There is no intersection.
        if (c == INTERVAL_PRECEDES) {
            return "INTERVAL_PRECEDES";
        }

        if (c == INTERVAL_PRECEDES_COULD_UNION) {
            return "INTERVAL_PRECEDES_COULD_UNION";
        }

        if (c == INTERVAL_SUCCEEDS) {
            return "INTERVAL_SUCCEEDS";
        }

        if (c == INTERVAL_UNKNOWN) {
            return "INTERVAL_UNKNOWN";
        }

        return "NO IDEA DUDE";
    }

    void Interval::reverse() {
        std::swap(start, end);
        std::swap(startInclusive, endInclusive);
    }

} // namespace mongo
