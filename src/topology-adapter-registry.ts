import { ConnectionClosedError } from "./errors.js";
import type { NativeAdapterOptions } from "./adapter-types.js";
import { NativeAdapter } from "./native-adapter.js";
import { mapSettledWithConcurrency } from "./topology-utilities.js";

/** Owns topology adapter creation, deduplication, and graceful retirement. */
export class TopologyAdapterRegistry {
  private readonly creations = new Map<string, Promise<NativeAdapter>>();
  private readonly adapters = new Map<string, NativeAdapter>();
  private readonly retiring = new Set<NativeAdapter>();

  constructor(
    private readonly assertOpen: () => void,
    private readonly isClosed: () => boolean
  ) {}

  async get(
    key: string,
    url: string,
    options: NativeAdapterOptions,
    retryUnavailable = true
  ): Promise<NativeAdapter> {
    this.assertOpen();
    const existing = this.adapters.get(key);
    if (existing != null) {
      if (!existing.isUnavailable) return existing;
      if (this.adapters.get(key) === existing) this.adapters.delete(key);
      this.retire(existing);
    }

    let creation = this.creations.get(key);
    if (creation == null) {
      const started = (async () => {
        const adapter = await NativeAdapter.fromUrl(url, options);
        if (this.isClosed()) {
          await adapter.close();
          this.assertOpen();
        }
        this.adapters.set(key, adapter);
        return adapter;
      })();
      const tracked = started.finally(() => {
        if (this.creations.get(key) === tracked) this.creations.delete(key);
      });
      creation = tracked;
      this.creations.set(key, creation);
    }

    const adapter = await creation;
    if (!adapter.isUnavailable) return adapter;
    if (this.creations.get(key) === creation) this.creations.delete(key);
    if (this.adapters.get(key) === adapter) this.adapters.delete(key);
    this.retire(adapter);
    if (!retryUnavailable) throw new ConnectionClosedError("unsent");
    return await this.get(key, url, options, false);
  }

  retireRemoved(active: ReadonlySet<string>, isCurrentlyActive: (key: string) => boolean): void {
    for (const [key, adapter] of this.adapters.entries()) {
      if (active.has(key)) continue;
      if (this.adapters.get(key) === adapter) this.adapters.delete(key);
      this.retire(adapter);
    }
    for (const [key, creation] of this.creations.entries()) {
      if (active.has(key)) continue;
      void creation.then((adapter) => {
        if (isCurrentlyActive(key)) return;
        if (this.adapters.get(key) === adapter) this.adapters.delete(key);
        this.retire(adapter);
      }).catch(() => undefined);
    }
  }

  async close(concurrency: number): Promise<void> {
    const adapters = [...new Set([...this.adapters.values(), ...this.retiring])];
    const creations = [...this.creations.values()];
    this.adapters.clear();
    this.retiring.clear();
    await mapSettledWithConcurrency(
      [
        ...adapters.map((adapter) => async () => await adapter.close()),
        ...creations.map((creation) => async () => await (await creation).close())
      ],
      concurrency,
      async (close) => await close()
    );
    this.creations.clear();
  }

  private retire(adapter: NativeAdapter): void {
    if (this.retiring.has(adapter)) return;
    this.retiring.add(adapter);
    void adapter.retire().then(
      () => this.retiring.delete(adapter),
      () => this.retiring.delete(adapter)
    );
  }
}
