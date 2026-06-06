# Security Policy

## Supported Versions

FerricStore TypeScript SDK is currently pre-1.0. Security fixes target the latest published minor version.

## Reporting A Vulnerability

Please report security issues privately by email to the FerricStore maintainers or through GitHub private vulnerability reporting when enabled on the repository.

Include:

- affected SDK version or commit;
- FerricStore server version or commit;
- whether ACLs, TLS, payloads, workflow lease tokens, or value refs are involved;
- a minimal reproduction if possible.

Do not open a public issue for suspected vulnerabilities.

## Client-Side Notes

- Treat `leaseToken` and `fencingToken` as authority to mutate claimed Flows.
- Avoid logging payloads, value refs, lease tokens, or credentials.
- Use FerricStore ACL/TLS/protected-mode settings for production.
- Codecs are SDK-side serialization only; they do not encrypt data.
