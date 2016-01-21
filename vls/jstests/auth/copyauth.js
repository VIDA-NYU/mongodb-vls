// Test copyDatabase command with various combinations of authed/unauthed source and dest.
var runTest = function() {
TestData.authMechanism = "MONGODB-CR"; // SERVER-11428
DB.prototype._defaultAuthenticationMechanism = "MONGODB-CR"; // SERVER-11428

ports = allocatePorts( 2 );

var baseName = "jstests_clone_copyauth";

// test copyDatabase from an auth enabled source, with an unauthed dest.
var source = startMongod( "--auth", "--port", ports[ 0 ], "--dbpath", MongoRunner.dataPath + baseName + "_source", "--nohttpinterface", "--bind_ip", "127.0.0.1", "--smallfiles" );
var target = startMongod( "--port", ports[ 1 ], "--dbpath", MongoRunner.dataPath + baseName + "_target", "--nohttpinterface", "--bind_ip", "127.0.0.1", "--smallfiles" );

source.getDB( "admin" ).createUser({user: "super", pwd: "super", roles: jsTest.adminUserRoles});
source.getDB( "admin" ).auth( "super", "super" );
source.getDB( baseName )[ baseName ].save( {i:1} );
source.getDB( baseName ).createUser({user: "foo", pwd: "bar", roles: jsTest.basicUserRoles});
source.getDB( "admin" ).logout();

assert.throws( function() { source.getDB( baseName )[ baseName ].findOne(); } );

assert.commandWorked(target.getDB(baseName).copyDatabase(baseName,
                                                         baseName,
                                                         source.host,
                                                         "foo",
                                                         "bar"));
assert.eq( 1, target.getDB( baseName )[ baseName ].count() );
assert.eq( 1, target.getDB( baseName )[ baseName ].findOne().i );

// Test with auth-enabled source and dest
stopMongod( ports[ 1 ] );
var target = startMongod( "--auth", "--port", ports[ 1 ], "--dbpath", MongoRunner.dataPath + baseName + "_target", "--nohttpinterface", "--bind_ip", "127.0.0.1", "--smallfiles" );

target.getDB( "admin" ).createUser({user: "super1", pwd: "super1", roles: jsTest.adminUserRoles});
assert.throws( function() { source.getDB( baseName )[ baseName ].findOne(); } );
target.getDB( "admin" ).auth( "super1", "super1" );

target.getDB(baseName).dropDatabase();
assert.eq(0, target.getDB(baseName)[baseName].count());
assert.commandWorked(target.getDB(baseName).copyDatabase(baseName,
                                                         baseName,
                                                         source.host,
                                                         "foo",
                                                         "bar"));
assert.eq( 1, target.getDB( baseName )[ baseName ].count() );
assert.eq( 1, target.getDB( baseName )[ baseName ].findOne().i );

// Test with auth-enabled dest, but un-authed source
stopMongod(ports[0]);
var source = startMongod( "--port", ports[ 0 ],
                          "--dbpath", MongoRunner.dataPath + baseName + "_source",
                          "--nohttpinterface",
                          "--bind_ip", "127.0.0.1",
                          "--smallfiles" );

// Need to re-insert doc as data will be cleared by the restart
source.getDB(baseName)[baseName].save({i:1});
assert.eq(1, source.getDB(baseName)[baseName].count());
assert.eq(1, source.getDB(baseName)[baseName].findOne().i);

target.getDB(baseName).dropDatabase();
assert.eq(0, target.getDB(baseName)[baseName].count());
assert.commandWorked(target.getDB(baseName).copyDatabase(baseName, baseName, source.host));
assert.eq(1, target.getDB(baseName)[baseName].count());
assert.eq(1, target.getDB(baseName)[baseName].findOne().i);

}

runTest();
