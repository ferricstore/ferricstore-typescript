# Release Process

Releases are published to npm from GitHub Actions when a version tag is pushed.

## Prerequisites

- Repository secret `NPM_TOKEN` is configured with publish access.
- `package.json` version and `CHANGELOG.md` are updated.
- `npm run check` and `npm run pack:dry-run` pass locally.
- The GitHub Actions `test` and `security` workflows pass on `main`.

## Release Steps

1. Update `package.json` version.
2. Move the changelog section from `Unreleased` to the release date.
3. Commit the release change.
4. Create the version tag:

   ```bash
   git tag v0.12.1
   git push origin main --tags
   ```

5. GitHub Actions verifies the tag matches the package version and checked-out
   commit, then runs `npm run check`, package dry-run, and
   `npm publish --provenance`.
6. GitHub Actions creates a GitHub release with generated release notes from the tag.

## Dry Run

```bash
npm run check
npm run pack:dry-run
```
