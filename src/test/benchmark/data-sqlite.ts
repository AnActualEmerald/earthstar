import * as Earthstar from "../../../mod.ts";
import { bench, runBenchmarks } from "https://deno.land/std@0.126.0/testing/bench.ts";
import { encode } from "https://deno.land/std@0.126.0/encoding/base64.ts";
import { prettyBytes } from "https://deno.land/std@0.126.0/fmt/bytes.ts";

/*
This benchmark does the following:

- Creates a Sqlite Replica
- Reads a 100kb image
	- And encodes it to base64
- Reads a 5mb image
	- And encodes it to base64

- Then we benchmark the following, 100 times:
	- Writing the base64ed image to the replica
	- Writing the base64ed BIG image to the replica
	- Write 5 text docs to the replica
	- Query all .txt docs from the replica
	- Query all docs from the replica

- Then we write the results to bench-results.txt
*/
async function runSqliteBlobBenchmarks() {
    const driver = new Earthstar.ReplicaDriverSqlite({
        mode: "create-or-open",
        filename: "./bench-blob.sql",
        share: "+blob.a123",
    });

    const replica = new Earthstar.Replica("+blob.a123", Earthstar.FormatValidatorEs4, driver);

    const keypair = await Earthstar.Crypto.generateAuthorKeypair("suzy") as Earthstar.AuthorKeypair;

    const imageData = await Deno.readFile("./src/test/benchmark/image.png");

    const bigImageData = await Deno.readFile("./src/test/benchmark/big-image.jpeg");
    const imageStat = await Deno.stat("./src/test/benchmark/image.png");
    const imageSize = imageStat.size;

    const base64ImageData = encode(imageData);
    const base64Size = new Blob([base64ImageData]).size;
    const bigImageStat = await Deno.stat("./src/test/benchmark/big-image.jpeg");
    const bigImageSize = bigImageStat.size;

    const bigBase64ImageData = encode(bigImageData);
    const bigBase64Size = new Blob([bigBase64ImageData]).size;

    const imageCount = 100;

    for (let i = 0; i <= imageCount; i++) {
        bench({
            name: `Write text doc (Step ${i})`,
            func: async (b) => {
                b.start();
                await replica.set(keypair, {
                    content: `Hi ${i}!`,
                    format: "es.4",
                    path: `/text/${i}.txt`,
                });
                b.stop();
            },
        });

        bench({
            name: `Write image doc (Step ${i})`,
            func: async (b) => {
                b.start();
                await replica.set(keypair, {
                    content: base64ImageData,
                    format: "es.4",
                    path: `/images/${i}.png`,
                });
                b.stop();
            },
        });

        bench({
            name: `Write big doc (Step ${i})`,
            func: async (b) => {
                b.start();
                await replica.set(keypair, {
                    content: bigBase64ImageData,
                    format: "es.4",
                    path: `/images/${i}-big.jpg`,
                });
                b.stop();
            },
        });

        bench({
            name: `Query .txt docs (Step ${i})`,
            func: async (b) => {
                b.start();
                await replica.queryDocs({
                    filter: {
                        pathEndsWith: ".txt",
                    },
                });
                b.stop();
            },
        });

        bench({
            name: `Query .png docs (Step ${i})`,
            func: async (b) => {
                b.start();
                await replica.queryDocs({
                    filter: {
                        pathEndsWith: ".png",
                    },
                });
                b.stop();
            },
        });

        bench({
            name: `Query .jpg docs (Step ${i})`,
            func: async (b) => {
                b.start();
                await replica.queryDocs({
                    filter: {
                        pathEndsWith: ".jpg",
                    },
                });
                b.stop();
            },
        });

        bench({
            name: `Get single image doc (Step ${i})`,
            func: async (b) => {
                b.start();
                await replica.getLatestDocAtPath(`/images/${i}.png`);
                b.stop();
            },
        });

        bench({
            name: `Get single BIG image doc (Step ${i})`,
            func: async (b) => {
                b.start();
                await replica.getLatestDocAtPath(`/images/${i}-big.jpg`);
                b.stop();
            },
        });

        bench({
            name: `Read all docs (Step ${i})`,
            func: async (b) => {
                b.start();
                await replica.getAllDocs();
                b.stop();
            },
        });
    }

    const { results } = await runBenchmarks();

    const sqliteStat = await Deno.stat("./bench-blob.sql");

    const testChunks = sliceIntoChunks(results, 9);

    let text = "";

    testChunks.forEach((results, i) => {
        text += `==== STEP ${i} ====
`;

        results.forEach((result) => {
            text += `	${result.name}: ${Math.round(result.totalMs)}ms
`;
        });

        text += `
`;
    });

    const idealBytes = (imageSize * imageCount) + (bigImageSize * imageCount);
    const totalBytes = (base64Size * imageCount) + (bigBase64Size * imageCount);

    text += `==== SIZE BREAKDOWN ====
	Ideal bytes stored (raw): ${prettyBytes(idealBytes)} 
	Actual bytes stored (base64): ${prettyBytes(totalBytes)}
	Final .sqlite Size: ${prettyBytes(sqliteStat.size)}
		`;

    await Deno.writeTextFile("./bench-results.txt", text);

    await replica.close(true);
}

function sliceIntoChunks<T>(arr: T[], chunkSize: number) {
    const res = [];
    for (let i = 0; i < arr.length; i += chunkSize) {
        const chunk = arr.slice(i, i + chunkSize);
        res.push(chunk);
    }
    return res;
}

await runSqliteBlobBenchmarks();
