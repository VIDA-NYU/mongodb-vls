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

#include "mongo/db/query/index_bounds_builder.h"

#include <limits>
#include "mongo/db/geo/geoconstants.h"
#include "mongo/db/geo/s2common.h"
#include "mongo/db/index/expression_index.h"
#include "mongo/db/matcher/expression_geo.h"
#include "mongo/db/query/indexability.h"
#include "mongo/db/query/qlog.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/db/geo/s2.h"
#include "third_party/s2/s2cell.h"
#include "third_party/s2/s2regioncoverer.h"

namespace mongo {

    string IndexBoundsBuilder::simpleRegex(const char* regex, const char* flags,
                                           BoundsTightness* tightnessOut) {
        string r = "";
        *tightnessOut = IndexBoundsBuilder::INEXACT_COVERED;

        bool multilineOK;
        if ( regex[0] == '\\' && regex[1] == 'A') {
            multilineOK = true;
            regex += 2;
        }
        else if (regex[0] == '^') {
            multilineOK = false;
            regex += 1;
        }
        else {
            return r;
        }

        bool extended = false;
        while (*flags) {
            switch (*(flags++)) {
            case 'm': // multiline
                if (multilineOK)
                    continue;
                else
                    return r;
            case 'x': // extended
                extended = true;
                break;
            default:
                return r; // cant use index
            }
        }

        stringstream ss;

        while(*regex) {
            char c = *(regex++);
            if ( c == '*' || c == '?' ) {
                // These are the only two symbols that make the last char optional
                r = ss.str();
                r = r.substr( 0 , r.size() - 1 );
                return r; //breaking here fails with /^a?/
            }
            else if (c == '|') {
                // whole match so far is optional. Nothing we can do here.
                return string();
            }
            else if (c == '\\') {
                c = *(regex++);
                if (c == 'Q'){
                    // \Q...\E quotes everything inside
                    while (*regex) {
                        c = (*regex++);
                        if (c == '\\' && (*regex == 'E')){
                            regex++; //skip the 'E'
                            break; // go back to start of outer loop
                        }
                        else {
                            ss << c; // character should match itself
                        }
                    }
                }
                else if ((c >= 'A' && c <= 'Z') ||
                        (c >= 'a' && c <= 'z') ||
                        (c >= '0' && c <= '0') ||
                        (c == '\0')) {
                    // don't know what to do with these
                    r = ss.str();
                    break;
                }
                else {
                    // slash followed by non-alphanumeric represents the following char
                    ss << c;
                }
            }
            else if (strchr("^$.[()+{", c)) {
                // list of "metacharacters" from man pcrepattern
                r = ss.str();
                break;
            }
            else if (extended && c == '#') {
                // comment
                r = ss.str();
                break;
            }
            else if (extended && isspace(c)) {
                continue;
            }
            else {
                // self-matching char
                ss << c;
            }
        }

        if ( r.empty() && *regex == 0 ) {
            r = ss.str();
            *tightnessOut = r.empty() ? IndexBoundsBuilder::INEXACT_COVERED : IndexBoundsBuilder::EXACT;
        }

        return r;
    }


    // static
    void IndexBoundsBuilder::allValuesForField(const BSONElement& elt, OrderedIntervalList* out) {
        // ARGH, BSONValue would make this shorter.
        BSONObjBuilder bob;
        bob.appendMinKey("");
        bob.appendMaxKey("");
        out->name = elt.fieldName();
        out->intervals.push_back(makeRangeInterval(bob.obj(), true, true));
    }

    Interval IndexBoundsBuilder::allValues() {
        BSONObjBuilder bob;
        bob.appendMinKey("");
        bob.appendMaxKey("");
        return makeRangeInterval(bob.obj(), true, true);
    }

    bool IntervalComparison(const Interval& lhs, const Interval& rhs) {
        int wo = lhs.start.woCompare(rhs.start, false);
        if (0 != wo) {
            return wo < 0;
        }

        // The start and end are equal.
        // Strict weak requires irreflexivity which implies that equivalence returns false.
        if (lhs.startInclusive == rhs.startInclusive) { return false; }

        // Put the bound that's inclusive to the left.
        return lhs.startInclusive;
    }

    // static
    void IndexBoundsBuilder::translateAndIntersect(const MatchExpression* expr,
                                                   const BSONElement& elt,
                                                   const IndexEntry& index,
                                                   OrderedIntervalList* oilOut,
                                                   BoundsTightness* tightnessOut) {
        OrderedIntervalList arg;
        translate(expr, elt, index, &arg, tightnessOut);

        // translate outputs arg in sorted order.  intersectize assumes that its arguments are
        // sorted.
        intersectize(arg, oilOut);
    }

    // static
    void IndexBoundsBuilder::translateAndUnion(const MatchExpression* expr,
                                               const BSONElement& elt,
                                               const IndexEntry& index,
                                               OrderedIntervalList* oilOut,
                                               BoundsTightness* tightnessOut) {
        translate(expr, elt, index, oilOut, tightnessOut);
        unionize(oilOut);
    }

    bool typeMatch(const BSONObj& obj) {
        BSONObjIterator it(obj);
        verify(it.more());
        BSONElement first = it.next();
        verify(it.more());
        BSONElement second = it.next();
        return first.canonicalType() == second.canonicalType();
    }

    // static
    void IndexBoundsBuilder::translate(const MatchExpression* expr,
                                       const BSONElement& elt,
                                       const IndexEntry& index,
                                       OrderedIntervalList* oilOut,
                                       BoundsTightness* tightnessOut) {
        oilOut->name = elt.fieldName();

        bool isHashed = false;
        if (mongoutils::str::equals("hashed", elt.valuestrsafe())) {
            isHashed = true;
        }

        if (isHashed) {
            verify(MatchExpression::EQ == expr->matchType()
                   || MatchExpression::MATCH_IN == expr->matchType());
        }

        if (MatchExpression::ELEM_MATCH_VALUE == expr->matchType()) {
            OrderedIntervalList acc;
            translate(expr->getChild(0), elt, index, &acc, tightnessOut);

            for (size_t i = 1; i < expr->numChildren(); ++i) {
                OrderedIntervalList next;
                BoundsTightness tightness;
                translate(expr->getChild(i), elt, index, &next, &tightness);
                intersectize(next, &acc);
            }

            for (size_t i = 0; i < acc.intervals.size(); ++i) {
                oilOut->intervals.push_back(acc.intervals[i]);
            }

            if (!oilOut->intervals.empty()) {
                std::sort(oilOut->intervals.begin(), oilOut->intervals.end(), IntervalComparison);
            }

            // $elemMatch value requires an array.
            // Scalars and directly nested objects are not matched with $elemMatch.
            // We can't tell if a multi-key index key is derived from an array field.
            // Therefore, a fetch is required.
            *tightnessOut = IndexBoundsBuilder::INEXACT_FETCH;
        }
        else if (MatchExpression::EQ == expr->matchType()) {
            const EqualityMatchExpression* node = static_cast<const EqualityMatchExpression*>(expr);
            translateEquality(node->getData(), isHashed, oilOut, tightnessOut);
        }
        else if (MatchExpression::LTE == expr->matchType()) {
            const LTEMatchExpression* node = static_cast<const LTEMatchExpression*>(expr);
            BSONElement dataElt = node->getData();

            // Everything is <= MaxKey.
            if (MaxKey == dataElt.type()) {
                oilOut->intervals.push_back(allValues());
                *tightnessOut = IndexBoundsBuilder::EXACT;
                return;
            }

            BSONObjBuilder bob;
            // Use -infinity for one-sided numerical bounds
            if (dataElt.isNumber()) {
                bob.appendNumber("", -std::numeric_limits<double>::infinity());
            }
            else {
                bob.appendMinForType("", dataElt.type());
            }
            bob.appendAs(dataElt, "");
            BSONObj dataObj = bob.obj();
            verify(dataObj.isOwned());
            oilOut->intervals.push_back(makeRangeInterval(dataObj, typeMatch(dataObj), true));
            // XXX: only exact if not (null or array)
            *tightnessOut = IndexBoundsBuilder::EXACT;
        }
        else if (MatchExpression::LT == expr->matchType()) {
            const LTMatchExpression* node = static_cast<const LTMatchExpression*>(expr);
            BSONElement dataElt = node->getData();

            // Everything is <= MaxKey.
            if (MaxKey == dataElt.type()) {
                oilOut->intervals.push_back(allValues());
                *tightnessOut = IndexBoundsBuilder::EXACT;
                return;
            }

            BSONObjBuilder bob;
            // Use -infinity for one-sided numerical bounds
            if (dataElt.isNumber()) {
                bob.appendNumber("", -std::numeric_limits<double>::infinity());
            }
            else {
                bob.appendMinForType("", dataElt.type());
            }
            bob.appendAs(dataElt, "");
            BSONObj dataObj = bob.obj();
            verify(dataObj.isOwned());
            Interval interval = makeRangeInterval(dataObj, typeMatch(dataObj), false);

            // If the operand to LT is equal to the lower bound X, the interval [X, X) is invalid
            // and should not be added to the bounds.
            if (!interval.isNull()) {
                oilOut->intervals.push_back(interval);
            }

            // XXX: only exact if not (null or array)
            *tightnessOut = IndexBoundsBuilder::EXACT;
        }
        else if (MatchExpression::GT == expr->matchType()) {
            const GTMatchExpression* node = static_cast<const GTMatchExpression*>(expr);
            BSONElement dataElt = node->getData();

            // Everything is > MinKey.
            if (MinKey == dataElt.type()) {
                oilOut->intervals.push_back(allValues());
                *tightnessOut = IndexBoundsBuilder::EXACT;
                return;
            }

            BSONObjBuilder bob;
            bob.appendAs(node->getData(), "");
            if (dataElt.isNumber()) {
                bob.appendNumber("", std::numeric_limits<double>::infinity());
            }
            else {
                bob.appendMaxForType("", dataElt.type());
            }
            BSONObj dataObj = bob.obj();
            verify(dataObj.isOwned());
            Interval interval = makeRangeInterval(dataObj, false, typeMatch(dataObj));

            // If the operand to GT is equal to the upper bound X, the interval (X, X] is invalid
            // and should not be added to the bounds.
            if (!interval.isNull()) {
                oilOut->intervals.push_back(interval);
            }

            // XXX: only exact if not (null or array)
            *tightnessOut = IndexBoundsBuilder::EXACT;
        }
        else if (MatchExpression::GTE == expr->matchType()) {
            const GTEMatchExpression* node = static_cast<const GTEMatchExpression*>(expr);
            BSONElement dataElt = node->getData();

            // Everything is >= MinKey.
            if (MinKey == dataElt.type()) {
                oilOut->intervals.push_back(allValues());
                *tightnessOut = IndexBoundsBuilder::EXACT;
                return;
            }

            BSONObjBuilder bob;
            bob.appendAs(dataElt, "");
            if (dataElt.isNumber()) {
                bob.appendNumber("", std::numeric_limits<double>::infinity());
            }
            else {
                bob.appendMaxForType("", dataElt.type());
            }
            BSONObj dataObj = bob.obj();
            verify(dataObj.isOwned());

            oilOut->intervals.push_back(makeRangeInterval(dataObj, true, typeMatch(dataObj)));
            // XXX: only exact if not (null or array)
            *tightnessOut = IndexBoundsBuilder::EXACT;
        }
        else if (MatchExpression::REGEX == expr->matchType()) {
            const RegexMatchExpression* rme = static_cast<const RegexMatchExpression*>(expr);
            translateRegex(rme, oilOut, tightnessOut);
        }
        else if (MatchExpression::MOD == expr->matchType()) {
            BSONObjBuilder bob;
            bob.appendMinForType("", NumberDouble);
            bob.appendMaxForType("", NumberDouble);
            BSONObj dataObj = bob.obj();
            verify(dataObj.isOwned());
            oilOut->intervals.push_back(makeRangeInterval(dataObj, true, true));
            *tightnessOut = IndexBoundsBuilder::INEXACT_COVERED;
        }
        else if (MatchExpression::TYPE_OPERATOR == expr->matchType()) {
            const TypeMatchExpression* tme = static_cast<const TypeMatchExpression*>(expr);
            BSONObjBuilder bob;
            bob.appendMinForType("", tme->getData());
            bob.appendMaxForType("", tme->getData());
            BSONObj dataObj = bob.obj();
            verify(dataObj.isOwned());
            oilOut->intervals.push_back(makeRangeInterval(dataObj, true, true));
            *tightnessOut = IndexBoundsBuilder::INEXACT_FETCH;
        }
        else if (MatchExpression::MATCH_IN == expr->matchType()) {
            const InMatchExpression* ime = static_cast<const InMatchExpression*>(expr);
            const ArrayFilterEntries& afr = ime->getData();

            *tightnessOut = IndexBoundsBuilder::EXACT;

            // Create our various intervals.

            IndexBoundsBuilder::BoundsTightness tightness;
            for (BSONElementSet::iterator it = afr.equalities().begin();
                 it != afr.equalities().end(); ++it) {
                translateEquality(*it, isHashed, oilOut, &tightness);
                if (tightness != IndexBoundsBuilder::EXACT) {
                    *tightnessOut = tightness;
                }
            }

            for (size_t i = 0; i < afr.numRegexes(); ++i) {
                translateRegex(afr.regex(i), oilOut, &tightness);
                if (tightness != IndexBoundsBuilder::EXACT) {
                    *tightnessOut = tightness;
                }
            }

            // XXX: what happens here?
            if (afr.hasNull()) { }
            // XXX: what happens here as well?
            if (afr.hasEmptyArray()) { }

            unionize(oilOut);
        }
        else if (MatchExpression::GEO == expr->matchType()) {
            const GeoMatchExpression* gme = static_cast<const GeoMatchExpression*>(expr);
            // Can only do this for 2dsphere.
            if (!mongoutils::str::equals("2dsphere", elt.valuestrsafe())) {
                warning() << "Planner error trying to build geo bounds for " << elt.toString()
                          << " index element.";
                verify(0);
            }

            const S2Region& region = gme->getGeoQuery().getRegion();
            ExpressionMapping::cover2dsphere(region, index.infoObj, oilOut);
            *tightnessOut = IndexBoundsBuilder::INEXACT_FETCH;
        }
        else {
            warning() << "Planner error, trying to build bounds for expr "
                      << expr->toString() << endl;
            verify(0);
        }
    }

    // static
    Interval IndexBoundsBuilder::makeRangeInterval(const BSONObj& obj, bool startInclusive,
                                                   bool endInclusive) {
        Interval ret;
        ret._intervalData = obj;
        ret.startInclusive = startInclusive;
        ret.endInclusive = endInclusive;
        BSONObjIterator it(obj);
        verify(it.more());
        ret.start = it.next();
        verify(it.more());
        ret.end = it.next();
        return ret;
    }

    // static
    void IndexBoundsBuilder::intersectize(const OrderedIntervalList& arg,
                                          OrderedIntervalList* oilOut) {
        verify(arg.name == oilOut->name);

        size_t argidx = 0;
        const vector<Interval>& argiv = arg.intervals;

        size_t ividx = 0;
        vector<Interval>& iv = oilOut->intervals;

        vector<Interval> result;

        while (argidx < argiv.size() && ividx < iv.size()) {
            Interval::IntervalComparison cmp = argiv[argidx].compare(iv[ividx]);
            /*
            QLOG() << "comparing " << argiv[argidx].toString()
                 << " with " << iv[ividx].toString()
                 << " cmp is " << Interval::cmpstr(cmp) << endl;
                 */

            verify(Interval::INTERVAL_UNKNOWN != cmp);

            if (cmp == Interval::INTERVAL_PRECEDES
                || cmp == Interval::INTERVAL_PRECEDES_COULD_UNION) {
                // argiv is before iv.  move argiv forward.
                ++argidx;
            }
            else if (cmp == Interval::INTERVAL_SUCCEEDS) {
                // iv is before argiv.  move iv forward.
                ++ividx;
            }
            else {
                // argiv[argidx] (cmpresults) iv[ividx]
                Interval newInt = argiv[argidx];
                newInt.intersect(iv[ividx], cmp);
                result.push_back(newInt);

                if (Interval::INTERVAL_EQUALS == cmp) {
                    ++argidx;
                    ++ividx;
                }
                else if (Interval::INTERVAL_WITHIN == cmp) {
                    ++argidx;
                }
                else if (Interval::INTERVAL_CONTAINS == cmp) {
                    ++ividx;
                }
                else if (Interval::INTERVAL_OVERLAPS_BEFORE == cmp) {
                    ++argidx;
                }
                else if (Interval::INTERVAL_OVERLAPS_AFTER == cmp) {
                    ++ividx;
                }
                else {
                    verify(0);
                }
            }
        }
        // XXX swap
        oilOut->intervals = result;
    }

    // static
    void IndexBoundsBuilder::unionize(OrderedIntervalList* oilOut) {
        vector<Interval>& iv = oilOut->intervals;

        // This can happen.
        if (iv.empty()) { return; }

        // Step 1: sort.
        std::sort(iv.begin(), iv.end(), IntervalComparison);

        // Step 2: Walk through and merge.
        size_t i = 0;
        while (i < iv.size() - 1) {
            // Compare i with i + 1.
            Interval::IntervalComparison cmp = iv[i].compare(iv[i + 1]);
            // QLOG() << "comparing " << iv[i].toString() << " with " << iv[i+1].toString()
                 // << " cmp is " << Interval::cmpstr(cmp) << endl;

            // This means our sort didn't work.
            verify(Interval::INTERVAL_SUCCEEDS != cmp);

            // Intervals are correctly ordered.
            if (Interval::INTERVAL_PRECEDES == cmp) {
                // We can move to the next pair.
                ++i;
            }
            else if (Interval::INTERVAL_EQUALS == cmp || Interval::INTERVAL_WITHIN == cmp) {
                // Interval 'i' is equal to i+1, or is contained within i+1.
                // Remove interval i and don't move to the next value of 'i'.
                iv.erase(iv.begin() + i);
            }
            else if (Interval::INTERVAL_CONTAINS == cmp) {
                // Interval 'i' contains i+1, remove i+1 and don't move to the next value of 'i'.
                iv.erase(iv.begin() + i + 1);
            }
            else if (Interval::INTERVAL_OVERLAPS_BEFORE == cmp
                     || Interval::INTERVAL_PRECEDES_COULD_UNION == cmp) {
                // We want to merge intervals i and i+1.
                // Interval 'i' starts before interval 'i+1'.
                BSONObjBuilder bob;
                bob.appendAs(iv[i].start, "");
                bob.appendAs(iv[i + 1].end, "");
                BSONObj data = bob.obj();
                bool startInclusive = iv[i].startInclusive;
                bool endInclusive = iv[i + 1].endInclusive;
                iv.erase(iv.begin() + i);
                // iv[i] is now the former iv[i + 1]
                iv[i] = makeRangeInterval(data, startInclusive, endInclusive);
                // Don't increment 'i'.
            }
        }
    }

    // static
    Interval IndexBoundsBuilder::makeRangeInterval(const string& start, const string& end,
                                                   bool startInclusive, bool endInclusive) {
        BSONObjBuilder bob;
        bob.append("", start);
        bob.append("", end);
        return makeRangeInterval(bob.obj(), startInclusive, endInclusive);
    }

    // static
    Interval IndexBoundsBuilder::makePointInterval(const BSONObj& obj) {
        Interval ret;
        ret._intervalData = obj;
        ret.startInclusive = ret.endInclusive = true;
        ret.start = ret.end = obj.firstElement();
        return ret;
    }

    // static
    Interval IndexBoundsBuilder::makePointInterval(const string& str) {
        BSONObjBuilder bob;
        bob.append("", str);
        return makePointInterval(bob.obj());
    }

    // static
    BSONObj IndexBoundsBuilder::objFromElement(const BSONElement& elt) {
        BSONObjBuilder bob;
        bob.appendAs(elt, "");
        return bob.obj();
    }

    // static
    void IndexBoundsBuilder::reverseInterval(Interval* ival) {
        BSONElement tmp = ival->start;
        ival->start = ival->end;
        ival->end = tmp;

        bool tmpInc = ival->startInclusive;
        ival->startInclusive = ival->endInclusive;
        ival->endInclusive = tmpInc;
    }

    // static
    void IndexBoundsBuilder::translateRegex(const RegexMatchExpression* rme,
                                            OrderedIntervalList* oilOut, BoundsTightness* tightnessOut) {

        const string start = simpleRegex(rme->getString().c_str(), rme->getFlags().c_str(), tightnessOut);

        // QLOG() << "regex bounds start is " << start << endl;
        // Note that 'tightnessOut' is set by simpleRegex above.
        if (!start.empty()) {
            string end = start;
            end[end.size() - 1]++;
            oilOut->intervals.push_back(makeRangeInterval(start, end, true, false));
        }
        else {
            BSONObjBuilder bob;
            bob.appendMinForType("", String);
            bob.appendMaxForType("", String);
            BSONObj dataObj = bob.obj();
            verify(dataObj.isOwned());
            oilOut->intervals.push_back(makeRangeInterval(dataObj, true, false));
        }

        // Regexes are after strings.
        BSONObjBuilder bob;
        bob.appendRegex("", rme->getString(), rme->getFlags());
        oilOut->intervals.push_back(makePointInterval(bob.obj()));
    }

    // static
    void IndexBoundsBuilder::translateEquality(const BSONElement& data, bool isHashed,
                                               OrderedIntervalList* oil, BoundsTightness* tightnessOut) {
        // We have to copy the data out of the parse tree and stuff it into the index
        // bounds.  BSONValue will be useful here.
        if (Array != data.type()) {
            BSONObj dataObj;
            if (isHashed) {
                dataObj = ExpressionMapping::hash(data);
            }
            else {
                dataObj = objFromElement(data);
            }

            verify(dataObj.isOwned());
            oil->intervals.push_back(makePointInterval(dataObj));
            // XXX: it's exact if the index isn't sparse?
            if (dataObj.firstElement().isNull() || isHashed) {
                *tightnessOut = IndexBoundsBuilder::INEXACT_FETCH;
            }
            else {
                *tightnessOut = IndexBoundsBuilder::EXACT;
            }
        }
        // In the following cases, 'data' is an array. Using
        // arrays with hashed indices is currently not supported,
        // so we don't have to worry about that case.
        else if (data.Obj().isEmpty()) { // Array == data.type()
            // XXX: tighten bounds in empty case
            oil->intervals.push_back(allValues());
            *tightnessOut = IndexBoundsBuilder::INEXACT_FETCH;
        }
        else { // Array == data.type() && !data.Obj().isEmpty()
            BSONObj dataObj = objFromElement(data);
            BSONElement firstEl = data.Obj().firstElement();

            // Use the first element in the array to construct the
            // first interval. (Using the first is arbitrary; we could
            // just as well use any array element.). If the query is
            // {a: [1, 2, 3]}, for example, then using the bounds [1, 1]
            // for the multikey index will pick up every document containing
            // the array [1, 2, 3].
            oil->intervals.push_back(makePointInterval(objFromElement(firstEl)));

            // The second point interval uses the entire array. This is
            // necessary so that the query {a: [1, 2, 3]} will match
            // documents like {a: [[1, 2, 3], 4, 5]}.
            oil->intervals.push_back(makePointInterval(dataObj));

            *tightnessOut = IndexBoundsBuilder::INEXACT_FETCH;
        }
    }

}  // namespace mongo
