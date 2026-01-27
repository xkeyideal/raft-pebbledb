## Why

Upgrade Pebble from v1.1.2 to v2.1.4 to adopt 2 years of accumulated improvements, bug fixes, and performance optimizations. The v2.x series is actively maintained and used in production by CockroachDB 25.3.0, providing better stability and modern features. This upgrade is essential for keeping the library dependencies current and ensuring long-term maintainability.

## What Changes

- **BREAKING**: Update `github.com/cockroachdb/pebble` dependency from v1.1.2 to v2.1.4 in go.mod
- Update transitive dependencies to versions compatible with Pebble v2.1.4
- Verify all existing API usage remains compatible (no code changes expected)
- Run full test suite to ensure behavioral consistency
- Execute benchmark tests to validate performance characteristics
- Update README.md to document the v2.x usage and format compatibility notes

## Capabilities

### New Capabilities
- `dependency-upgrade`: Manages the process of upgrading the Pebble dependency from v1.x to v2.x, including dependency resolution and compatibility verification

### Modified Capabilities
<!-- No existing capabilities are being modified - this is a dependency upgrade only -->

## Impact

**Dependencies**:
- go.mod: Pebble v1.1.2 → v2.1.4
- Potential transitive dependency updates (cockroachdb/errors, cockroachdb/redact, golang.org/x/*, etc.)

**Code**:
- No source code changes expected (using stable core APIs)
- pebble_store.go, config.go - API usage verified compatible
- bench_test.go, pebble_store_test.go - test suite validation required

**Documentation**:
- README.md: Add v2.x usage notes and format compatibility warning for end users

**User Impact**:
- Users of this framework will need to be aware that Pebble v2.x does not support the oldest disk formats
- Existing databases created with v1.x may require format migration before upgrading (user's responsibility)
- No API changes for framework consumers
