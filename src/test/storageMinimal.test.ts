import t = require('tap');
//t.runOnly = true;

import {
    AuthorKeypair,
    IValidator,
    isErr,
} from '../util/types';
import {
    generateAuthorKeypair,
} from '../crypto/crypto';
import { ValidatorEs4 } from '../validator/es4';

import {
    IStorage,
    IStorageAsync,
} from '../storage/storageTypes';
import {
    StorageMinimal
} from '../storage/storageMinimal';
import { StorageToAsync } from '../storage/storageToAsync';

import {
    Scenario,
    runStorageTestsForScenario,
} from './storageAsyncTests.setup'

//================================================================================
// prepare for test scenarios

let WORKSPACE = '+gardenclub.xxxxxxxxxxxxxxxxxxxx';

let VALIDATORS : IValidator[] = [ValidatorEs4];

// tests assume these are in alphabetical order by author shortname
let keypair1 = generateAuthorKeypair('aut1') as AuthorKeypair;
let keypair2 = generateAuthorKeypair('aut2') as AuthorKeypair;
let keypair3 = generateAuthorKeypair('aut3') as AuthorKeypair;
let keypair4 = generateAuthorKeypair('aut4') as AuthorKeypair;
if (isErr(keypair1)) { throw "oops"; }
if (isErr(keypair2)) { throw "oops"; }
if (isErr(keypair3)) { throw "oops"; }
if (isErr(keypair4)) { throw "oops"; }
let now = 1500000000000000;

let SEC = 1000000;
let MIN = SEC * 60;
let HOUR = MIN * 60;
let DAY = HOUR * 24;

//================================================================================
// CONSTRUCTOR TESTS

t.test(`StorageMinimal: constructor success`, (t: any) => {
    let storage = new StorageMinimal(VALIDATORS, WORKSPACE);
    t.same(storage.workspace, WORKSPACE, 'it is working');
    storage.close();
    t.end();
});

t.test(`StorageMinimal: constructor errors`, (t: any) => {
    t.throws(() => new StorageMinimal([], WORKSPACE), 'throws when no validators are provided');
    t.throws(() => new StorageMinimal(VALIDATORS, 'bad-workspace-address'), 'throws when workspace address is invalid');
    t.end();
});

t.test(`Async'd StorageMinimal: constructor`, (t: any) => {
    t.throws(() => new StorageToAsync(new StorageMinimal([], WORKSPACE)), 'throws when no validators are provided');
    t.throws(() => new StorageToAsync(new StorageMinimal(VALIDATORS, 'bad-workspace-address')), 'throws when workspace address is invalid');
    t.end();
});

//================================================================================
// MAIN TESTS

let scenarios : Scenario[] = [
    {
        makeStorage: (workspace : string) : IStorage => {
            let storage = new StorageMinimal(VALIDATORS, workspace);
            storage._now = now;
            return storage;
        },
        description: "StorageMinimal",
    },
    /*
    {
        makeStorage: (workspace : string) : IStorageAsync => {
            let storage = new StorageToAsync(new StorageMinimal(VALIDATORS, workspace), 10);
            storage._now = now;
            return storage;
        },
        description: "Async'd StorageMinimal",
    },
    */
];

for (let scenario of scenarios) {
    runStorageTestsForScenario(scenario);
}
