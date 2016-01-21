// pdfiletests.cpp : pdfile unit tests.
//

/**
 *    Copyright (C) 2008 10gen Inc.
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

#include "mongo/pch.h"

#include "mongo/db/db.h"
#include "mongo/db/json.h"
#include "mongo/db/pdfile.h"
#include "mongo/db/ops/insert.h"
#include "mongo/db/structure/collection.h"
#include "mongo/dbtests/dbtests.h"

namespace PdfileTests {

    namespace Insert {
        class Base {
        public:
            Base() : _context( ns() ) {
            }
            virtual ~Base() {
                if ( !nsd() )
                    return;
                _context.db()->dropCollection( ns() );
            }
        protected:
            static const char *ns() {
                return "unittests.pdfiletests.Insert";
            }
            static NamespaceDetails *nsd() {
                return nsdetails( ns() );
            }

            Lock::GlobalWrite lk_;
            Client::Context _context;
        };

        class InsertNoId : public Base {
        public:
            void run() {
                BSONObj x = BSON( "x" << 1 );
                ASSERT( x["_id"].type() == 0 );
                Collection* collection = _context.db()->getOrCreateCollection( ns() );
                StatusWith<DiskLoc> dl = collection->insertDocument( x, true );
                ASSERT( !dl.isOK() );

                StatusWith<BSONObj> fixed = fixDocumentForInsert( x );
                ASSERT( fixed.isOK() );
                x = fixed.getValue();
                ASSERT( x["_id"].type() == jstOID );
                dl = collection->insertDocument( x, true );
                ASSERT( dl.isOK() );
            }
        };

        class UpdateDate : public Base {
        public:
            void run() {
                BSONObjBuilder b;
                b.appendTimestamp( "a" );
                b.append( "_id", 1 );
                BSONObj o = b.done();

                BSONObj fixed = fixDocumentForInsert( o ).getValue();
                ASSERT_EQUALS( 2, fixed.nFields() );
                ASSERT( fixed.firstElement().fieldNameStringData() == "_id" );
                ASSERT( fixed.firstElement().number() == 1 );

                BSONElement a = fixed["a"];
                ASSERT( o["a"].type() == Timestamp );
                ASSERT( o["a"].timestampValue() == 0 );
                ASSERT( a.type() == Timestamp );
                ASSERT( a.timestampValue() > 0 );
            }
        };
    } // namespace Insert

    class ExtentSizing {
    public:
        struct SmallFilesControl {
            SmallFilesControl() {
                old = storageGlobalParams.smallfiles;
                storageGlobalParams.smallfiles = false;
            }
            ~SmallFilesControl() {
                storageGlobalParams.smallfiles = old;
            }
            bool old;
        };
        void run() {
            SmallFilesControl c;

            ASSERT_EQUALS( Extent::maxSize(),
                           ExtentManager::quantizeExtentSize( Extent::maxSize() ) );

            // test that no matter what we start with, we always get to max extent size
            for ( int obj=16; obj<BSONObjMaxUserSize; obj += 111 ) {

                int sz = Extent::initialSize( obj );

                double totalExtentSize = sz;

                int numFiles = 1;
                int sizeLeftInExtent = Extent::maxSize() - 1;

                for ( int i=0; i<100; i++ ) {
                    sz = Extent::followupSize( obj , sz );
                    ASSERT( sz >= obj );
                    ASSERT( sz >= Extent::minSize() );
                    ASSERT( sz <= Extent::maxSize() );
                    ASSERT( sz <= DataFile::maxSize() );

                    totalExtentSize += sz;

                    if ( sz < sizeLeftInExtent ) {
                        sizeLeftInExtent -= sz;
                    }
                    else {
                        numFiles++;
                        sizeLeftInExtent = Extent::maxSize() - sz;
                    }
                }
                ASSERT_EQUALS( Extent::maxSize() , sz );

                double allocatedOnDisk = (double)numFiles * Extent::maxSize();

                ASSERT( ( totalExtentSize / allocatedOnDisk ) > .95 );

            }
        }
    };

    class All : public Suite {
    public:
        All() : Suite( "pdfile" ) {}

        void setupTests() {
            add< Insert::InsertNoId >();
            add< Insert::UpdateDate >();
            add< ExtentSizing >();
        }
    } myall;

} // namespace PdfileTests

