/** A runtime read-only facade over a map whose backing storage stays private. */
class ReadonlyMapView<K, V> implements ReadonlyMap<K, V> {
  readonly #source: ReadonlyMap<K, V>;

  constructor(source: ReadonlyMap<K, V>) {
    this.#source = source;
  }

  get size(): number {
    return this.#source.size;
  }

  entries() {
    return this.#source.entries();
  }

  forEach(
    callback: (value: V, key: K, map: ReadonlyMap<K, V>) => void,
    thisArg?: unknown
  ): void {
    for (const [key, value] of this.#source) callback.call(thisArg, value, key, this);
  }

  get(key: K): V | undefined {
    return this.#source.get(key);
  }

  has(key: K): boolean {
    return this.#source.has(key);
  }

  keys() {
    return this.#source.keys();
  }

  values() {
    return this.#source.values();
  }

  [Symbol.iterator]() {
    return this.#source[Symbol.iterator]();
  }
}

export function readonlyMapView<K, V>(source: ReadonlyMap<K, V>): ReadonlyMap<K, V> {
  return Object.freeze(new ReadonlyMapView(source));
}
