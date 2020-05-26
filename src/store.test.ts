import * as fs from 'fs';
import t = require('tap');
import { addSigilToKey, generateKeypair } from './crypto';
import { SyncOpts, Item, FormatName, AuthorKey, IStore, IValidator } from './types';
import { ValidatorKw1 } from "./validatorKw1";
import { StoreMemory } from './storeMemory';
import { StoreSqlite } from './storeSqlite';

//================================================================================
// prepare for test scenarios

let WORKSPACE = 'gardenclub';

let FORMAT : FormatName = 'kw.1';
let VALIDATORS : IValidator[] = [ValidatorKw1];

let keypair1 = generateKeypair();
let keypair2 = generateKeypair();
let keypair3 = generateKeypair();
let author1: AuthorKey = addSigilToKey(keypair1.public);
let author2: AuthorKey = addSigilToKey(keypair2.public);
let author3: AuthorKey = addSigilToKey(keypair3.public);
let now = 1500000000000000;

interface Scenario {
    makeStore: (workspace : string) => IStore,
    description: string,
}
let scenarios : Scenario[] = [
    {
        makeStore: (workspace : string) : IStore => new StoreMemory(VALIDATORS, workspace),
        description: 'StoreMemory',
    },
    {
        makeStore: (workspace : string) : IStore => new StoreSqlite({
            mode: 'create',
            workspace: workspace,
            validators: VALIDATORS,
            filename: ':memory:'
        }),
        description: "StoreSqlite(':memory:')",
    },
];

//================================================================================
// memory specific tests

t.test(`StoreMemory: constructor`, (t: any) => {
    t.throws(() => new StoreMemory([], WORKSPACE), 'throws when no validators are provided');
    t.done();
});

//================================================================================
// sqlite specific tests

// TODO: test constructor with different opts

t.test(`StoreSqlite: opts: workspace and filename requirements`, (t: any) => {
    let fn : string;
    let clearFn = (fn : string) => {
        if (fs.existsSync(fn)) { fs.unlinkSync(fn); }
    }
    let touchFn = (fn : string) => { fs.writeFileSync(fn, 'foo'); }

    // create with :memory:
    t.throws(() => new StoreSqlite({
        mode: 'create',
        workspace: null,
        validators: VALIDATORS,
        filename: ':memory:'
    }), 'create mode throws when workspace is null, :memory:');
    t.doesNotThrow(() => new StoreSqlite({
        mode: 'create',
        workspace: WORKSPACE,
        validators: VALIDATORS,
        filename: ':memory:'
    }), 'create mode works when workspace is provided, :memory:');
    t.throws(() => new StoreSqlite({
        mode: 'create',
        workspace: WORKSPACE,
        validators: [],
        filename: ':memory:'
    }), 'create mode fails when no validators are provided');

    // create with real filename
    fn = 'testtesttest1.db';
    clearFn(fn);
    t.doesNotThrow(() => new StoreSqlite({
        mode: 'create',
        workspace: WORKSPACE,
        validators: VALIDATORS,
        filename: fn,
    }), 'create mode works when workspace is provided and a real filename');
    t.ok(fs.existsSync(fn), 'create mode created a file');
    clearFn(fn);

    // create with existing filename
    fn = 'testtesttest1b.db';
    touchFn(fn);
    t.throws(() => new StoreSqlite({
        mode: 'create',
        workspace: WORKSPACE,
        validators: VALIDATORS,
        filename: fn,
    }), 'create mode throws when pointed at existing file');
    clearFn(fn);

    // open and :memory:
    t.throws(() => new StoreSqlite({
        mode: 'open',
        workspace: WORKSPACE,
        validators: VALIDATORS,
        filename: ':memory:',
    }), 'open mode throws with :memory: and a workspace');
    t.throws(() => new StoreSqlite({
        mode: 'open',
        workspace: null,
        validators: VALIDATORS,
        filename: ':memory:',
    }), 'open mode throws with :memory: and null workspace');

    // open missing filename
    t.throws(() => new StoreSqlite({
        mode: 'open',
        workspace: WORKSPACE,
        validators: VALIDATORS,
        filename: 'xxx',
    }), 'open mode throws when file does not exist');

    // open and real but missing filename
    fn = 'testtesttest2.db';
    clearFn(fn);
    t.throws(() => new StoreSqlite({
        mode: 'open',
        workspace: WORKSPACE,
        validators: VALIDATORS,
        filename: fn,
    }), 'open mode throws when workspace is provided and file does not exist');
    clearFn(fn);
    t.throws(() => new StoreSqlite({
        mode: 'open',
        workspace: null,
        validators: VALIDATORS,
        filename: fn,
    }), 'open mode throws when workspace is null and file does not exist');
    clearFn(fn);

    // create-or-open :memory:
    t.doesNotThrow(() => new StoreSqlite({
        mode: 'create-or-open',
        workspace: WORKSPACE,
        validators: VALIDATORS,
        filename: ':memory:'
    }), 'create-or-open mode works when workspace is provided');
    t.throws(() => new StoreSqlite({
        mode: 'create-or-open',
        workspace: null,
        validators: VALIDATORS,
        filename: ':memory:'
    }), 'create-or-open mode throws when workspace is null');

    // create-or-open: create then open real file
    fn = 'testtesttest3.db';
    clearFn(fn);
    t.doesNotThrow(() => new StoreSqlite({
        mode: 'create-or-open',
        workspace: WORKSPACE,
        validators: VALIDATORS,
        filename: fn,
    }), 'create-or-open mode works when creating a real file');
    t.ok(fs.existsSync(fn), 'create-or-open mode created a file');
    t.throws(() => new StoreSqlite({
        mode: 'create-or-open',
        workspace: 'xxx',
        validators: VALIDATORS,
        filename: fn,
    }), 'create-or-open mode fails when opening existing file with mismatched workspace');
    t.doesNotThrow(() => new StoreSqlite({
        mode: 'create-or-open',
        workspace: WORKSPACE,
        validators: VALIDATORS,
        filename: fn,
    }), 'create-or-open mode works when opening a real file with matching workspace');
    clearFn(fn);

    // open: create then open real file
    fn = 'testtesttest4.db';
    clearFn(fn);
    t.doesNotThrow(() => new StoreSqlite({
        mode: 'create',
        workspace: WORKSPACE,
        validators: VALIDATORS,
        filename: fn,
    }), 'creating a real file');
    t.ok(fs.existsSync(fn), 'file was created');
    t.throws(() => new StoreSqlite({
        mode: 'open',
        workspace: 'xxx',
        validators: VALIDATORS,
        filename: fn,
    }), 'open throws when workspace does not match');
    t.doesNotThrow(() => new StoreSqlite({
        mode: 'open',
        workspace: WORKSPACE,
        validators: VALIDATORS,
        filename: fn,
    }), 'open works when workspace matches');
    t.doesNotThrow(() => new StoreSqlite({
        mode: 'open',
        workspace: null,
        validators: VALIDATORS,
        filename: fn,
    }), 'open works when workspace is null');
    clearFn(fn);

    // unrecognized mode
    t.throws(() => new StoreSqlite({
        mode: 'xxx' as any,
        workspace: null,
        validators: VALIDATORS,
        filename: ':memory:'
    }), 'constructor throws with unrecognized mode');

    t.done();
});

t.test(`StoreSqlite: config`, (t: any) => {
    let kw = new StoreSqlite({
        mode: 'create',
        workspace: WORKSPACE,
        validators: VALIDATORS,
        filename: ':memory:'
    });
    t.equal(kw._getConfig('foo'), null);
    kw._setConfig('foo', 'bar');
    t.equal(kw._getConfig('foo'), 'bar');
    kw._setConfig('foo', 'baz');
    t.equal(kw._getConfig('foo'), 'baz');
    t.done();
});


//================================================================================
// run the standard store tests on each scenario

for (let scenario of scenarios) {
    t.test(`==== starting test of ====${scenario.description}`, (t: any) => {
        t.done();
    });

    t.test(scenario.description + ': empty store', (t: any) => {
        let kw = scenario.makeStore(WORKSPACE);
        t.same(kw.keys(), [], 'no keys');
        t.same(kw.items(), [], 'no items');
        t.same(kw.values(), [], 'no values');
        t.equal(kw.getItem('xxx'), undefined, 'getItem undefined');
        t.equal(kw.getValue('xxx'), undefined, 'getValue undefined');
        t.same(kw.authors(), [], 'no authors');
        t.done();
    });

    t.test(scenario.description + ': store ingestItem rejects invalid items', (t: any) => {
        let kw = scenario.makeStore(WORKSPACE);

        let item1: Item = {
            format: FORMAT,
            workspace: WORKSPACE,
            key: 'k1',
            value: 'v1',
            timestamp: now,
            author: author1,
            signature: 'xxx',
        };
        let signedItem = ValidatorKw1.signItem(item1, keypair1.secret);
        t.ok(kw.ingestItem(signedItem), "successful ingestion");
        t.equal(kw.getValue('k1'), 'v1', "getValue worked");

        t.notOk(kw.ingestItem(item1), "don't ingest: bad signature");
        t.notOk(kw.ingestItem({...signedItem, format: 'xxx'}), "don't ingest: unknown format");
        t.notOk(kw.ingestItem({...signedItem, timestamp: now / 1000}), "don't ingest: timestamp too small, probably in milliseconds");
        t.notOk(kw.ingestItem({...signedItem, timestamp: now * 2}), "don't ingest: timestamp in future");
        t.notOk(kw.ingestItem({...signedItem, timestamp: Number.MAX_SAFE_INTEGER * 2}), "don't ingest: timestamp way too large");
        t.notOk(kw.ingestItem({...signedItem, workspace: 'xxx'}), "don't ingest: changed workspace after signing");

        let signedItemDifferentWorkspace = ValidatorKw1.signItem({...item1, workspace: 'xxx'}, keypair1.secret);
        t.notOk(kw.ingestItem(signedItemDifferentWorkspace), "don't ingest: mismatch workspace");

        t.notOk(kw.set({
            format: 'xxx',
            key: 'k1',
            value: 'v1',
            author: author1,
            authorSecret: keypair1.secret,
        }), 'set rejects unknown format');

        let writableKeys = [
            'hello',
            '~' + author1 + '/about',
            'chat/~@notme.ed25519~' + author1,
        ];
        for (let key of writableKeys) {
            t.ok(kw.ingestItem(
                ValidatorKw1.signItem(
                    {...item1, key: key},
                    keypair1.secret
                )),
                "do ingest: writable key " + key
            );
        }
        let notWritableKeys = [
            '~@notme.ed25519/about',
            '~',
        ];
        for (let key of notWritableKeys) {
            t.notOk(kw.ingestItem(
                ValidatorKw1.signItem(
                    {...item1, key: key},
                    keypair1.secret
                )),
                "don't ingest: non-writable key " + key
            );
        }

        t.done();
    });

    t.test(scenario.description + ': one-author store', (t: any) => {
        let kw = scenario.makeStore(WORKSPACE);
        t.equal(kw.getValue('key1'), undefined, 'nonexistant keys are undefined');
        t.equal(kw.getValue('key2'), undefined, 'nonexistant keys are undefined');

        // set a decoy key to make sure the later tests return the correct key
        t.ok(kw.set({format: FORMAT, key: 'decoy', value:'zzz', author: author1, authorSecret: keypair1.secret, timestamp: now}), 'set decoy key');

        t.ok(kw.set({format: FORMAT, key: 'key1', value: 'val1.0', author: author1, authorSecret: keypair1.secret, timestamp: now}), 'set new key');
        t.equal(kw.getValue('key1'), 'val1.0');

        t.ok(kw.set({format: FORMAT, key: 'key1', value: 'val1.2', author: author1, authorSecret: keypair1.secret, timestamp: now + 2}), 'overwrite key with newer time');
        t.equal(kw.getValue('key1'), 'val1.2');

        // write with an old timestamp - this timestamp should be overridden to the existing timestamp + 1.
        // note that on ingest() the newer timestamp wins, but on set() we adjust the newly created timestamp
        // so it's always greater than the existing ones.
        t.ok(kw.set({format: FORMAT, key: 'key1', value: 'val1.1', author: author1, authorSecret: keypair1.secret, timestamp: now-99}), 'automatically supercede previous timestamp');
        t.equal(kw.getValue('key1'), 'val1.1', 'superceded newer existing value');
        t.equal(kw.getItem('key1')?.timestamp, now + 3, 'timestamp was superceded by 1 microsecond');

        //log('_items:', JSON.stringify(kw._items, null, 4));

        // should be alphabetical
        t.same(kw.keys(), ['decoy', 'key1'], 'keys() are correct');

        // order of values should match order of keys
        t.same(kw.values(), ['zzz', 'val1.1'], 'values() are correct');

        t.same(kw.authors(), [author1], 'author');

        t.done();
    });

    t.test(scenario.description + ': key queries', (t: any) => {
        let kw = scenario.makeStore(WORKSPACE);
        let keys = 'zzz aaa dir dir/ q qq qqq dir/a dir/b dir/c'.split(' ');
        let ii = 0;
        for (let key of keys) {
            t.ok(kw.set({format: FORMAT, key: key, value: 'true', author: author1, authorSecret: keypair1.secret, timestamp: now + ii}), 'set key: ' + key),
                ii += 1;
        }
        let sortedKeys = [...keys];
        sortedKeys.sort();
        let kwKeys = kw.keys();
        t.same(keys.length, kwKeys.length, 'same number of keys');
        t.same(sortedKeys, kwKeys, 'keys are sorted');
        t.same(kw.keys({ key: 'q' }), ['q'], 'query for specific key');
        t.same(kw.keys({ key: 'nope' }), [], 'query for missing key');
        t.same(kw.keys({ lowKey: 'q', highKey: 'qqq' }), ['q', 'qq'], 'lowKey <= k < highKey');
        t.same(kw.keys({ lowKey: 'q', highKey: 'qqq', limit: 1 }), ['q'], 'lowKey, highKey with limit');
        t.same(kw.keys({ prefix: 'dir/' }), ['dir/', 'dir/a', 'dir/b', 'dir/c'], 'prefix');
        t.same(kw.keys({ prefix: 'dir/', limit: 2 }), ['dir/', 'dir/a'], 'prefix with limit');
        t.done();
    });

    t.test(scenario.description + ': multi-author writes', (t: any) => {
        let kw = scenario.makeStore(WORKSPACE);

        // set decoy keys to make sure the later tests return the correct key
        t.ok(kw.set({format: FORMAT, key: 'decoy2', value: 'zzz', author: author1, authorSecret: keypair1.secret, timestamp: now}), 'set decoy key 2');
        t.ok(kw.set({format: FORMAT, key: 'decoy1', value: 'aaa', author: author1, authorSecret: keypair1.secret, timestamp: now}), 'set decoy key 1');

        t.ok(kw.set({format: FORMAT, key: 'key1', value: 'one', author: author1, authorSecret: keypair1.secret, timestamp: now}), 'set new key');
        t.equal(kw.getValue('key1'), 'one');

        // this will overwrite 'one' but the item for 'one' will remain in history.
        // history will have 2 items for this key.
        t.ok(kw.set({format: FORMAT, key: 'key1', value: 'two', author: author2, authorSecret: keypair2.secret, timestamp: now + 1}), 'update from a second author');
        t.equal(kw.getValue('key1'), 'two');

        // this will replace the old original item 'one' from this author.
        // history will have 2 items for this key.
        t.ok(kw.set({format: FORMAT, key: 'key1', value: 'three', author: author1, authorSecret: keypair1.secret, timestamp: now + 2}), 'update from original author again');
        t.equal(kw.getValue('key1'), 'three');

        //log('_items:', JSON.stringify(kw._items, null, 4));

        t.equal(kw.keys().length, 3, '3 keys');
        t.equal(kw.values().length, 3, '3 values');
        t.equal(kw.values({ includeHistory: true }).length, 4, '4 values with history');

        t.same(kw.keys(), ['decoy1', 'decoy2', 'key1'], 'keys()');
        t.same(kw.values(), ['aaa', 'zzz', 'three'], 'values()');
        t.same(kw.values({ includeHistory: true }), ['aaa', 'zzz', 'three', 'two'], 'values with history, newest first');

        t.same(
            kw.items({ includeHistory: true }).map((item : Item) => item.author),
            [author1, author1, author1, author2],
            'items with history, newest first, items should have correct authors'
        );

        let sortedAuthors = [author1, author2];
        sortedAuthors.sort();
        t.same(kw.authors(), sortedAuthors, 'authors');

        // TODO: test 2 authors, same timestamps, different signatures

        t.done();
    });

    t.test(scenario.description + ': sync: push to empty store', (t: any) => {
        let kw1 = scenario.makeStore(WORKSPACE);
        let kw2 = scenario.makeStore(WORKSPACE);

        // set up some keys
        t.ok(kw1.set({format: FORMAT, key: 'decoy2', value: 'zzz', author: author1, authorSecret: keypair1.secret, timestamp: now}), 'author1 set decoy key');
        t.ok(kw1.set({format: FORMAT, key: 'decoy1', value: 'aaa', author: author1, authorSecret: keypair1.secret, timestamp: now}), 'author1 set decoy key');
        t.ok(kw1.set({format: FORMAT, key: 'key1', value: 'one', author: author1, authorSecret: keypair1.secret, timestamp: now}), 'author1 set key1');
        t.ok(kw1.set({format: FORMAT, key: 'key1', value: 'two', author: author2, authorSecret: keypair2.secret, timestamp: now + 1}), 'author2 set key1');

        // sync
        let syncResults = kw1.sync(kw2, { direction: 'push', existing: true, live: false });
        //log('sync results', syncResults);
        t.same(syncResults, { numPushed: 4, numPulled: 0 }, 'pushed 4 items (includes history items).  pulled 0.');

        // check results
        t.same(kw1.keys(), kw2.keys(), 'kw1.keys() == kw2.keys()');
        t.same(kw1.values(), kw2.values(), 'kw1 values == kw2');
        t.same(kw1.values({ includeHistory: true }), kw2.values({ includeHistory: true }), 'kw1 values with history == kw2');

        t.same(kw2.keys(), ['decoy1', 'decoy2', 'key1'], 'keys are as expected');
        t.same(kw2.getValue('key1'), 'two', 'latest item for a key wins on kw2');
        t.same(kw2.getItem('key1')?.value, 'two', 'getItem has correct value');
        t.same(kw2.values(), ['aaa', 'zzz', 'two'], 'kw2 values are as expected');
        t.same(kw2.values({ includeHistory: true }), ['aaa', 'zzz', 'two', 'one'], 'values with history are as expected');

        // sync again.  nothing should happen.
        let syncResults2 = kw1.sync(kw2, { direction: 'push', existing: true, live: false });
        //log('sync results 2', syncResults2);
        t.same(syncResults2, { numPushed: 0, numPulled: 0 }, 'nothing should happen if syncing again');

        //log('kw1._items:', JSON.stringify(kw1._items, null, 4));
        //log('kw1.keys()', kw1.keys());
        //log('kw1.values()', kw1.values());
        //log('kw1.getItem("key1")', kw1.getItem('key1'));

        t.done();
    });

    t.test(scenario.description + ': sync: two-way', (t: any) => {

        let optsToTry : SyncOpts[] = [
            {},
            { direction: 'both', existing: true, live: false },  // these are the defaults
        ];

        for (let opts of optsToTry) {
            let kw1 = scenario.makeStore(WORKSPACE);
            let kw2 = scenario.makeStore(WORKSPACE);

            // set up some keys
            t.ok(kw1.set({format: FORMAT, key: 'decoy2', value: 'zzz', author: author1, authorSecret: keypair1.secret, timestamp: now}), 'author1 set decoy key');  // winner  (push #1)
            t.ok(kw1.set({format: FORMAT, key: 'decoy1', value: 'aaa', author: author1, authorSecret: keypair1.secret, timestamp: now}), 'author1 set decoy key');  // winner  (push 2)
            t.ok(kw1.set({format: FORMAT, key: 'key1', value: 'one', author: author1, authorSecret: keypair1.secret, timestamp: now}), 'author1 set key1');      // becomes history  (push 3)
            t.ok(kw1.set({format: FORMAT, key: 'key1', value: 'two', author: author2, authorSecret: keypair2.secret, timestamp: now + 1}), 'author2 set key1');  // winner  (push 4)

            t.ok(kw2.set({format: FORMAT, key: 'latestOnKw1', value: '221', author: author1, authorSecret: keypair1.secret, timestamp: now}));       // dropped
            t.ok(kw1.set({format: FORMAT, key: 'latestOnKw1', value: '111', author: author1, authorSecret: keypair1.secret, timestamp: now + 10}));  // winner  (push 5)

            t.ok(kw1.set({format: FORMAT, key: 'latestOnKw2', value: '11', author: author1, authorSecret: keypair1.secret, timestamp: now}));       // dropped
            t.ok(kw2.set({format: FORMAT, key: 'latestOnKw2', value: '22', author: author1, authorSecret: keypair1.secret, timestamp: now + 10}));  // winner  (pull 1)

            t.ok(kw1.set({format: FORMAT, key: 'authorConflict', value: 'author1kw1', author: author1, authorSecret: keypair1.secret, timestamp: now}));      // becomes history  (push 6)
            t.ok(kw2.set({format: FORMAT, key: 'authorConflict', value: 'author2kw2', author: author2, authorSecret: keypair2.secret, timestamp: now + 1}));  // winner  (pull 2)

            // sync
            let syncResults = kw1.sync(kw2, opts);
            //log('sync results', syncResults);
            t.same(syncResults, { numPushed: 6, numPulled: 2 }, 'pushed 6 items, pulled 2 (including history)');

            t.equal(kw1.keys().length, 6, '6 keys');
            t.equal(kw1.items().length, 6, '6 items');
            t.equal(kw1.items({ includeHistory: true }).length, 8, '8 items with history');
            t.equal(kw1.values().length, 6, '6 values');
            t.equal(kw1.values({ includeHistory: true }).length, 8, '8 values with history');

            t.same(kw1.keys(), 'authorConflict decoy1 decoy2 key1 latestOnKw1 latestOnKw2'.split(' '), 'correct keys on kw1');
            t.same(kw1.values(), 'author2kw2 aaa zzz two 111 22'.split(' '), 'correct values on kw1');

            t.same(kw1.keys(), kw2.keys(), 'keys match');
            t.same(kw1.items(), kw2.items(), 'items match');
            t.same(kw1.items({ includeHistory: true }), kw2.items({ includeHistory: true }), 'items with history: match');
            t.same(kw1.values(), kw2.values(), 'values match');
            t.same(kw1.values({ includeHistory: true }), kw2.values({ includeHistory: true }), 'values with history: match');
        }

        t.done();
    });

    t.test(scenario.description + ': sync: mismatched workspaces', (t: any) => {
        let kwA1 = scenario.makeStore('a');
        let kwA2 = scenario.makeStore('a');
        let kwB = scenario.makeStore('b');
        t.ok(kwA1.set({format: FORMAT, key: 'a1', value: 'a1', author: author1, authorSecret: keypair1.secret}));
        t.ok(kwA2.set({format: FORMAT, key: 'a2', value: 'a2', author: author1, authorSecret: keypair1.secret}));
        t.ok(kwB.set({format: FORMAT, key: 'b', value: 'b', author: author1, authorSecret: keypair1.secret}));

        t.same(kwA1.sync(kwB), { numPulled: 0, numPushed: 0}, 'sync across different workspaces should do nothing');
        t.same(kwA1.sync(kwA2), { numPulled: 1, numPushed: 1}, 'sync across matching workspaces should do something');

        t.done();
    });

    t.test(scenario.description + ': sync: misc other options', (t: any) => {
        let kwEmpty1 = scenario.makeStore(WORKSPACE);
        let kwEmpty2 = scenario.makeStore(WORKSPACE);
        let kw = scenario.makeStore(WORKSPACE);

        // this time let's omit schema and timestamp
        t.ok(kw.set({format: FORMAT, key: 'foo', value: 'bar', author: author1, authorSecret: keypair1.secret}));

        // live mode (not implemented yet)
        t.throws(() => kwEmpty1.sync(kwEmpty2, {live: true}), 'live is not implemented yet and should throw');

        // sync with empty stores
        t.same(kwEmpty1.sync(kwEmpty2), { numPushed: 0, numPulled: 0 }, 'sync with empty stores');
        t.same(kwEmpty1.sync(kwEmpty2, {direction: 'push'}), { numPushed: 0, numPulled: 0 }, 'sync with empty stores');
        t.same(kwEmpty1.sync(kwEmpty2, {direction: 'pull'}), { numPushed: 0, numPulled: 0 }, 'sync with empty stores');
        t.same(kwEmpty1.sync(kwEmpty2, {direction: 'both'}), { numPushed: 0, numPulled: 0 }, 'sync with empty stores');
        t.same(kwEmpty1.sync(kwEmpty2, {existing: false}), { numPushed: 0, numPulled: 0 }, 'sync with empty stores');

        // sync with empty stores
        t.same(kw.sync(kwEmpty1, {direction: 'pull'}), { numPushed: 0, numPulled: 0 }, 'pull from empty store');
        t.same(kwEmpty1.sync(kw, {direction: 'push'}), { numPushed: 0, numPulled: 0 }, 'push to empty store');

        // sync with self
        t.same(kw.sync(kw), { numPushed: 0, numPulled: 0 }, 'sync with self should do nothing');

        // existing: false
        t.same(kw.sync(kwEmpty1, {existing: false}), { numPushed: 0, numPulled: 0 }, 'sync with existing: false does nothing');
        t.same(kwEmpty1.sync(kw, {existing: false}), { numPushed: 0, numPulled: 0 }, 'sync with existing: false does nothing');

        // successful sync
        t.same(kw.sync(kwEmpty1), { numPushed: 1, numPulled: 0 }, 'successful sync (push)');
        t.same(kwEmpty2.sync(kw), { numPushed: 0, numPulled: 1 }, 'successful sync (pull)');

        t.done();
    });
}