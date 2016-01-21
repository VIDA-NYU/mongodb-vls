/* Copyright 2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "mongo/tools/mongostat_options.h"

#include "mongo/bson/util/builder.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/options_parser/options_parser.h"
#include "mongo/util/options_parser/startup_options.h"

namespace {

    namespace moe = ::mongo::optionenvironment;

    TEST(Registration, RegisterAllOptions) {

        moe::OptionSection options;

        ASSERT_OK(::mongo::addMongoStatOptions(&options));

        std::vector<moe::OptionDescription> options_vector;
        ASSERT_OK(options.getAllOptions(&options_vector));

        for(std::vector<moe::OptionDescription>::const_iterator iterator = options_vector.begin();
            iterator != options_vector.end(); iterator++) {

            if (iterator->_dottedName == "help") {
                ASSERT_EQUALS(iterator->_singleName, "help");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "produce help message");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "verbose") {
                ASSERT_EQUALS(iterator->_singleName, "verbose,v");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "be more verbose (include multiple times for more verbosity e.g. -vvvvv)");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "quiet") {
                ASSERT_EQUALS(iterator->_singleName, "quiet");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "silence all non error diagnostic messages");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "version") {
                ASSERT_EQUALS(iterator->_singleName, "version");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "print the program's version and exit");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "vv") {
                ASSERT_EQUALS(iterator->_singleName, "vv");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "verbose");
                ASSERT_EQUALS(iterator->_isVisible, false);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "vvv") {
                ASSERT_EQUALS(iterator->_singleName, "vvv");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "verbose");
                ASSERT_EQUALS(iterator->_isVisible, false);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "vvvv") {
                ASSERT_EQUALS(iterator->_singleName, "vvvv");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "verbose");
                ASSERT_EQUALS(iterator->_isVisible, false);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "vvvvv") {
                ASSERT_EQUALS(iterator->_singleName, "vvvvv");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "verbose");
                ASSERT_EQUALS(iterator->_isVisible, false);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "vvvvvv") {
                ASSERT_EQUALS(iterator->_singleName, "vvvvvv");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "verbose");
                ASSERT_EQUALS(iterator->_isVisible, false);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "vvvvvvv") {
                ASSERT_EQUALS(iterator->_singleName, "vvvvvvv");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "verbose");
                ASSERT_EQUALS(iterator->_isVisible, false);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "vvvvvvvv") {
                ASSERT_EQUALS(iterator->_singleName, "vvvvvvvv");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "verbose");
                ASSERT_EQUALS(iterator->_isVisible, false);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "vvvvvvvvv") {
                ASSERT_EQUALS(iterator->_singleName, "vvvvvvvvv");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "verbose");
                ASSERT_EQUALS(iterator->_isVisible, false);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "vvvvvvvvvv") {
                ASSERT_EQUALS(iterator->_singleName, "vvvvvvvvvv");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "verbose");
                ASSERT_EQUALS(iterator->_isVisible, false);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "host") {
                ASSERT_EQUALS(iterator->_singleName, "host,h");
                ASSERT_EQUALS(iterator->_type, moe::String);
                ASSERT_EQUALS(iterator->_description, "mongo host to connect to ( <set name>/s1,s2 for sets)");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "port") {
                ASSERT_EQUALS(iterator->_singleName, "port");
                ASSERT_EQUALS(iterator->_type, moe::String);
                ASSERT_EQUALS(iterator->_description, "server port. Can also use --host hostname:port");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "ipv6") {
                ASSERT_EQUALS(iterator->_singleName, "ipv6");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "enable IPv6 support (disabled by default)");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "username") {
                ASSERT_EQUALS(iterator->_singleName, "username,u");
                ASSERT_EQUALS(iterator->_type, moe::String);
                ASSERT_EQUALS(iterator->_description, "username");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "password") {
                ASSERT_EQUALS(iterator->_singleName, "password,p");
                ASSERT_EQUALS(iterator->_type, moe::String);
                ASSERT_EQUALS(iterator->_description, "password");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                moe::Value implicitVal(std::string(""));
                ASSERT_TRUE(iterator->_implicit.equal(implicitVal));
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "authenticationDatabase") {
                ASSERT_EQUALS(iterator->_singleName, "authenticationDatabase");
                ASSERT_EQUALS(iterator->_type, moe::String);
                ASSERT_EQUALS(iterator->_description, "user source (defaults to dbname)");
                ASSERT_EQUALS(iterator->_isVisible, true);
                moe::Value defaultVal(std::string(""));
                ASSERT_TRUE(iterator->_default.equal(defaultVal));
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "authenticationMechanism") {
                ASSERT_EQUALS(iterator->_singleName, "authenticationMechanism");
                ASSERT_EQUALS(iterator->_type, moe::String);
                ASSERT_EQUALS(iterator->_description, "authentication mechanism");
                ASSERT_EQUALS(iterator->_isVisible, true);
                moe::Value defaultVal(std::string("MONGODB-CR"));
                ASSERT_TRUE(iterator->_default.equal(defaultVal));
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "noheaders") {
                ASSERT_EQUALS(iterator->_singleName, "noheaders");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "don't output column names");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "rowcount") {
                ASSERT_EQUALS(iterator->_singleName, "rowcount,n");
                ASSERT_EQUALS(iterator->_type, moe::Int);
                ASSERT_EQUALS(iterator->_description, "number of stats lines to print (0 for indefinite)");
                ASSERT_EQUALS(iterator->_isVisible, true);
                moe::Value defaultVal(0);
                ASSERT_TRUE(iterator->_default.equal(defaultVal));
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "http") {
                ASSERT_EQUALS(iterator->_singleName, "http");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "use http instead of raw db connection");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "discover") {
                ASSERT_EQUALS(iterator->_singleName, "discover");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "discover nodes and display stats for all");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "all") {
                ASSERT_EQUALS(iterator->_singleName, "all");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "all optional fields");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "sleep") {
                ASSERT_EQUALS(iterator->_singleName, "sleep");
                ASSERT_EQUALS(iterator->_type, moe::Int);
                ASSERT_EQUALS(iterator->_description, "seconds to sleep between samples");
                ASSERT_EQUALS(iterator->_isVisible, false);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceCommandLine);
                ASSERT_EQUALS(iterator->_positionalStart, 1);
                ASSERT_EQUALS(iterator->_positionalEnd, 1);
            }
#ifdef MONGO_SSL
            else if (iterator->_dottedName == "ssl") {
                ASSERT_EQUALS(iterator->_singleName, "ssl");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "use SSL for all connections");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "ssl.CAFile") {
                ASSERT_EQUALS(iterator->_singleName, "sslCAFile");
                ASSERT_EQUALS(iterator->_type, moe::String);
                ASSERT_EQUALS(iterator->_description, "Certificate Authority file for SSL");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "ssl.PEMKeyFile") {
                ASSERT_EQUALS(iterator->_singleName, "sslPEMKeyFile");
                ASSERT_EQUALS(iterator->_type, moe::String);
                ASSERT_EQUALS(iterator->_description, "PEM certificate/key file for SSL");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "ssl.PEMKeyPassword") {
                ASSERT_EQUALS(iterator->_singleName, "sslPEMKeyPassword");
                ASSERT_EQUALS(iterator->_type, moe::String);
                ASSERT_EQUALS(iterator->_description, "password for key in PEM file for SSL");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "ssl.CRLFile") {
                ASSERT_EQUALS(iterator->_singleName, "sslCRLFile");
                ASSERT_EQUALS(iterator->_type, moe::String);
                ASSERT_EQUALS(iterator->_description, "Certificate Revocation List file for SSL");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "ssl.allowInvalidCertificates") {
                ASSERT_EQUALS(iterator->_singleName, "sslAllowInvalidCertificates");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "allow connections to servers with invalid certificates");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
            else if (iterator->_dottedName == "ssl.FIPSMode") {
                ASSERT_EQUALS(iterator->_singleName, "sslFIPSMode");
                ASSERT_EQUALS(iterator->_type, moe::Switch);
                ASSERT_EQUALS(iterator->_description, "activate FIPS 140-2 mode at startup");
                ASSERT_EQUALS(iterator->_isVisible, true);
                ASSERT_TRUE(iterator->_default.isEmpty());
                ASSERT_TRUE(iterator->_implicit.isEmpty());
                ASSERT_EQUALS(iterator->_isComposing, false);
                ASSERT_EQUALS(iterator->_sources, moe::SourceAll);
                ASSERT_EQUALS(iterator->_positionalStart, -1);
                ASSERT_EQUALS(iterator->_positionalEnd, -1);
            }
#endif
            else {
                ::mongo::StringBuilder sb;
                sb << "Found extra option: " << iterator->_dottedName <<
                      " which we did not register";
                FAIL(sb.str());
            }
        }
    }

} // namespace
