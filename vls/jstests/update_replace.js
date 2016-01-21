// This test checks validation of the replaced doc (on the server) for dots, $prefix and _id

/**
 * Collection "class" that disables client side checks so we can test the server.
 */
var MyCollection = function(mongo, db, shortName, fullName) {
    this._mongo = mongo;
    this._db = db;
    this._shortName = shortName;
    this._fullName = fullName;
};

// Technique adopted from "Javascript Patterns" by Stoyan Stefanov
// Classical Pattern #5 - A Temporary Constructor
var Proxy = function() {};
Proxy.prototype = DBCollection.prototype;
MyCollection.prototype = new Proxy();
MyCollection.prototype._validateForStorage = function() {};

var shortName = 'jstests_update_replace';
var t = new MyCollection(db.getMongo(), db, shortName, db.getName() + '.' + shortName);
t.drop();

// Should not allow "." in field names
t.save({_id:1, "a.a":1})
assert.gleError(db, "a.a");

// Should not allow "." in field names, embedded
t.save({_id:1, a :{"a.a":1}})
assert.gleError(db, "a: a.a");

// Should not allow "$"-prefixed field names, caught before "." check
t.save({_id:1, $a :{"a.a":1}})
assert.gleError(db, "$a: a.a");

// Should not allow "$"-prefixed field names
t.save({_id:1, $a: 1})
assert.gleError(db, "$a");

// _id validation checks

// Should not allow regex _id
t.save({_id: /a/})
assert.gleError(db, "_id regex");

// Should not allow regex _id, even if not first
t.save({a:2, _id: /a/})
assert.gleError(db, "a _id regex");

// Should not allow array _id
t.save({_id: [9]})
assert.gleError(db, "_id array");

// This is fine since _id isn't a top level field
t.save({a :{ _id: [9]}})
assert.gleSuccess(db, "embedded _id array");

// This is fine since _id isn't a top level field
t.save({b:1, a :{ _id: [9]}})
assert.gleSuccess(db, "b embedded _id array");