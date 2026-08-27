import js from "@eslint/js";
import tseslint from "typescript-eslint";

const globals = {
  AbortController: "readonly",
  Buffer: "readonly",
  console: "readonly",
  process: "readonly",
  setTimeout: "readonly"
};

const tsRules = [
  js.configs.recommended,
  ...tseslint.configs.strictTypeChecked,
  ...tseslint.configs.stylisticTypeChecked
].map((config) => ({
  ...config,
  files: ["**/*.ts"]
}));

export default tseslint.config(
  {
    ignores: ["dist/**", "node_modules/**", "coverage/**", "docs/api/**", "docs/agent-api/**"]
  },
  {
    files: ["**/*.{js,mjs,cjs}"],
    ...js.configs.recommended,
    languageOptions: {
      globals
    }
  },
  ...tsRules,
  {
    files: ["**/*.ts"],
    languageOptions: {
      globals,
      parserOptions: {
        allowDefaultProject: ["vitest.config.ts"],
        projectService: true,
        tsconfigRootDir: import.meta.dirname
      }
    },
    rules: {
      "@typescript-eslint/no-confusing-void-expression": ["error", { ignoreArrowShorthand: true }],
      "@typescript-eslint/no-invalid-void-type": "off",
      "@typescript-eslint/no-non-null-assertion": "error",
      "@typescript-eslint/no-redundant-type-constituents": "off",
      "@typescript-eslint/no-unnecessary-condition": "off",
      "@typescript-eslint/no-unnecessary-type-parameters": "off",
      "@typescript-eslint/prefer-readonly": "error",
      "@typescript-eslint/require-await": "off",
      "@typescript-eslint/restrict-template-expressions": [
        "error",
        { allowBoolean: true, allowNumber: true, allowNullish: true }
      ],
      "@typescript-eslint/switch-exhaustiveness-check": "error"
    }
  }
);
