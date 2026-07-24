import { describe, expect, it } from "vitest";
import {
  FERRICSTORE_MINIMUM_SERVER_VERSION,
  FERRICSTORE_SDK_VERSION
} from "../src/version.js";

describe("0.10.3 projection release contract", () => {
  it("publishes a new SDK patch and requires the projection-capable server", () => {
    expect(FERRICSTORE_SDK_VERSION).toBe("0.4.1");
    expect(FERRICSTORE_MINIMUM_SERVER_VERSION).toBe("0.10.3");
  });
});
