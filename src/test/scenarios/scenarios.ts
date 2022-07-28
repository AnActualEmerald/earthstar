import { CryptoDriverSodium } from "../../crypto/crypto-driver-sodium.ts";
import { ICryptoDriver } from "../../crypto/crypto-types.ts";
import { DocDriverLocalStorage } from "../../replica/doc_drivers/localstorage.ts";
import { DocDriverSqliteFfi } from "../../replica/doc_drivers/sqlite_ffi.ts";
import { DocDriverScenario, PartnerScenario, Scenario } from "./types.ts";
import {
  universalCryptoDrivers,
  universalPartners,
  universalReplicaBlobDrivers,
  universalReplicaDocDrivers,
} from "./scenarios.universal.ts";
import { Syncer } from "../../syncer/syncer.ts";
import { IPeer } from "../../peer/peer-types.ts";
import { deferred } from "https://deno.land/std@0.138.0/async/deferred.ts";
import { serve } from "https://deno.land/std@0.129.0/http/server.ts";
import { FormatsArg } from "../../formats/default.ts";
import { DocDriverSqlite } from "../../replica/doc_drivers/sqlite.deno.ts";
import { IReplicaBlobDriver } from "../../replica/replica-types.ts";
import { BlobDriverFilesystem } from "../../replica/blob_drivers/filesystem.ts";
import { PartnerWebServer } from "../../syncer/partner_web_server.ts";
import { PartnerWebClient } from "../../syncer/partner_web_client.ts";

export const cryptoScenarios: Scenario<ICryptoDriver>[] = [
  ...universalCryptoDrivers,
  {
    name: "Sodium",
    item: CryptoDriverSodium,
  },
];

export const docDriverScenarios: Scenario<DocDriverScenario>[] = [
  ...universalReplicaDocDrivers,
  {
    name: "LocalStorage",
    item: {
      persistent: true,
      builtInConfigKeys: [],
      makeDriver: (addr, variant?: string) =>
        new DocDriverLocalStorage(addr, variant),
    },
  },
  {
    name: "Sqlite FFI",
    item: {
      persistent: true,
      builtInConfigKeys: ["schemaVersion", "share"],
      makeDriver: (addr, variant?: string) =>
        new DocDriverSqliteFfi({
          filename: `${addr}.${variant ? `${variant}.` : ""}ffi.sqlite`,
          mode: "create-or-open",
          share: addr,
        }),
    },
  },
  {
    name: "Sqlite",
    item: {
      persistent: true,
      builtInConfigKeys: ["schemaVersion", "share"],
      makeDriver: (addr, variant?: string) =>
        new DocDriverSqlite({
          filename: `${addr}.${variant ? `${variant}.` : ""}sqlite`,
          mode: "create-or-open",
          share: addr,
        }),
    },
  },
];

export const blobDriverScenarios: Scenario<() => IReplicaBlobDriver>[] = [
  ...universalReplicaBlobDrivers,
  {
    name: "Filesystem",
    item: () => new BlobDriverFilesystem("./src/test/tmp"),
  },
];

export class PartnerScenarioWeb<F> implements PartnerScenario<F> {
  private serve: Promise<void> | undefined;
  private abortController: AbortController;

  formats: FormatsArg<F>;

  constructor(formats: FormatsArg<F>) {
    this.formats = formats;
    this.abortController = new AbortController();
  }

  async setup(peerA: IPeer, peerB: IPeer) {
    const serverSyncerPromise = deferred<Syncer<WebSocket, F>>();

    const handler = (req: Request) => {
      // check what the url is... if it's for upload / download urls, parse the url, call the freaking syncer...

      const { socket, response } = Deno.upgradeWebSocket(req);

      const partner = new PartnerWebServer({ socket });

      serverSyncerPromise.resolve(
        new Syncer({
          partner,
          mode: "once",
          peer: peerB,
          formats: this.formats,
        }),
      );

      return response;
    };

    this.abortController = new AbortController();

    this.serve = serve(handler, {
      hostname: "0.0.0.0",
      port: 8083,
      signal: this.abortController.signal,
    });

    const clientSyncer = new Syncer({
      partner: new PartnerWebClient({
        url: "ws://localhost:8083",
      }),
      mode: "once",
      peer: peerA,
      formats: this.formats,
    });

    const serverSyncer = await serverSyncerPromise;

    return Promise.resolve(
      [clientSyncer, serverSyncer] as [
        Syncer<undefined, F>,
        Syncer<WebSocket, F>,
      ],
    );
  }

  teardown() {
    this.abortController.abort();

    return this.serve as Promise<void>;
  }
}

export const partnerScenarios: Scenario<
  <F>(
    formats: FormatsArg<F>,
  ) => PartnerScenario<F>
>[] = [...universalPartners, {
  name: "Web",
  item: (formats) => new PartnerScenarioWeb(formats),
}];
