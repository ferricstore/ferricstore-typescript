import type { CommandArgument } from "./internal.js";
import { decodeGeoSearchMembers, encodeGeoSearchArgs, geoSearchMetadataCount } from "./store-decoders.js";
import {
  commandArgs,
  concatArgs,
  denseArrayResponse,
  encode,
  encodedArgs,
  geoAddOptions,
  mapArrayResponse,
  number,
  requireNonEmpty,
  string
} from "./store-utilities.js";
import type { GeoAddOptions, GeoMember, StoreCommandClient } from "./store.js";

export class GeoStore {
  constructor(private readonly client: StoreCommandClient) {}

  async geoadd(key: string, members: GeoMember[], options: GeoAddOptions = {}): Promise<number> {
    requireNonEmpty(members, "GEOADD", "member");
    const args: CommandArgument[] = ["GEOADD", key, ...geoAddOptions(options)];
    for (let index = 0; index < members.length; index += 1) {
      if (!Object.hasOwn(members, index)) throw new TypeError("GEOADD members must be dense");
      const item = members[index];
      if (item == null) throw new TypeError("GEOADD members must contain member objects");
      if (
        !Object.hasOwn(item, "longitude") ||
        !Object.hasOwn(item, "latitude") ||
        !Object.hasOwn(item, "member")
      ) throw new TypeError("GEOADD members require own longitude, latitude, and member fields");
      args.push(item.longitude, item.latitude, encode(this.client.codec, item.member));
    }
    return number(await commandArgs(this.client, args));
  }

  geopos(key: string, member: unknown, ...members: unknown[]): Promise<([string, string] | null)[]>;
  geopos(key: string, ...members: unknown[]): Promise<([string, string] | null)[]> {
    return this.geoposMany(key, members);
  }

  async geoposMany(key: string, members: readonly unknown[]): Promise<([string, string] | null)[]> {
    const memberCount = members.length;
    return mapArrayResponse(
      await commandArgs(this.client, encodedArgs(["GEOPOS", key], this.client.codec, members)),
      memberCount,
      "GEOPOS",
      (position) => {
        if (position == null) return null;
        const coordinates = denseArrayResponse(position, 2, "GEOPOS");
        return [string(coordinates[0]), string(coordinates[1])];
      }
    );
  }

  async geodist(key: string, member1: unknown, member2: unknown, unit?: "m" | "km" | "mi" | "ft"): Promise<string | null> {
    const response = await this.client.command("GEODIST", key, encode(this.client.codec, member1), encode(this.client.codec, member2), ...(unit == null ? [] : [unit]));
    return response == null ? null : string(response);
  }

  geohash(key: string, member: unknown, ...members: unknown[]): Promise<(string | null)[]>;
  geohash(key: string, ...members: unknown[]): Promise<(string | null)[]> {
    return this.geohashMany(key, members);
  }

  async geohashMany(key: string, members: readonly unknown[]): Promise<(string | null)[]> {
    const memberCount = members.length;
    return mapArrayResponse(
      await commandArgs(this.client, encodedArgs(["GEOHASH", key], this.client.codec, members)),
      memberCount,
      "GEOHASH",
      (hash) => hash == null ? null : string(hash)
    );
  }

  async geosearch(key: string, args: CommandArgument[]): Promise<unknown[]> {
    requireNonEmpty(args, "GEOSEARCH");
    const metadataCount = geoSearchMetadataCount(args);
    const encodedSearchArgs = encodeGeoSearchArgs(this.client.codec, args);
    return decodeGeoSearchMembers(
      this.client.codec,
      await commandArgs(this.client, concatArgs(["GEOSEARCH", key], encodedSearchArgs)),
      metadataCount
    );
  }

  async geosearchstore(destination: string, source: string, args: CommandArgument[]): Promise<number> {
    requireNonEmpty(args, "GEOSEARCHSTORE");
    return number(
      await commandArgs(
        this.client,
        concatArgs(["GEOSEARCHSTORE", destination, source], encodeGeoSearchArgs(this.client.codec, args))
      )
    );
  }
}
