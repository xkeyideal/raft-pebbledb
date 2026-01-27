## Context

This is a major version upgrade of the Pebble key-value storage dependency from v1.1.2 to v2.1.4. The raft-pebbledb framework provides a Raft log storage implementation using Pebble as the backend. The current implementation uses stable core APIs (NewIter, Get, Set, Delete, Options) that are expected to remain compatible across the v1.x → v2.x transition.

**Current State:**
- Pebble v1.1.2 from 2024 (2 years old)
- No custom comparers, mergers, or block property collectors
- Focus on Sync/NoSync write options for durability control
- Comprehensive benchmark suite for performance validation

**Constraints:**
- This is a framework/library, not a production system with user data
- No need for data migration tooling (users handle their own data)
- Must maintain API compatibility for framework consumers
- Performance characteristics must remain acceptable

**Stakeholders:**
- Framework users who will need to understand v2.x format implications
- Maintainers ensuring long-term dependency health

## Goals / Non-Goals

**Goals:**
- Upgrade Pebble dependency from v1.1.2 to v2.1.4 in go.mod
- Resolve all transitive dependency updates
- Verify existing code compiles and tests pass with zero modifications
- Document v2.x format compatibility considerations in README
- Validate performance characteristics through benchmark comparison

**Non-Goals:**
- Data migration tooling (framework users manage their own data)
- Format version upgrade automation (v2.x format support only)
- API refactoring or feature additions beyond the dependency update
- Supporting both v1.x and v2.x simultaneously
- Providing backward compatibility shims

## Decisions

### Decision 1: Direct upgrade to v2.1.4 without intermediate versions

**Choice:** Upgrade directly from v1.1.2 to v2.1.4 (skipping v1.1.5 and v2.0.x).

**Rationale:**
- This is a framework without production data to migrate
- v2.1.4 is the latest stable version (used by CockroachDB 25.3.0)
- No intermediate versions required since we're not migrating data
- Simplifies the upgrade path to a single version jump

**Alternatives considered:**
- Two-step upgrade (v1.1.2 → v1.1.5 → v2.1.4): Unnecessary complexity for a framework without data migration needs
- Upgrade to v2.0.x first: v2.1.4 is more stable and battle-tested

### Decision 2: No source code modifications expected

**Choice:** Proceed with assumption that no code changes are needed, validate through testing.

**Rationale:**
- Code analysis shows usage of only stable core APIs
- No use of deprecated features or edge-case APIs
- Pebble v2.x maintains API compatibility for core operations
- If issues arise, they will surface during compilation or testing

**Alternatives considered:**
- Preemptive API audit: Overhead not justified given conservative API usage
- Gradual API migration: Not applicable since no breaking changes expected

### Decision 3: Document format compatibility in README, don't build migration tools

**Choice:** Add clear warnings in README about v2.x format incompatibility with oldest formats, but don't provide migration utilities.

**Rationale:**
- Framework users are responsible for their own data management
- Migration requirements vary by deployment (dev vs. production)
- Pebble provides `RatchetFormatMajorVersion` API for users who need it
- README guidance is sufficient for framework-level concerns

**Alternatives considered:**
- Include migration utility: Scope creep, users may have custom requirements
- Provide migration example code: Could be misleading since migration is deployment-specific

### Decision 4: Use benchmark suite for performance validation

**Choice:** Run existing benchmark suite to compare v1.1.2 vs v2.1.4 performance, document results.

**Rationale:**
- Framework already has comprehensive benchmarks (FirstIndex, LastIndex, Get, Set, etc.)
- Performance data provides confidence and transparency
- Any regressions can be identified and addressed
- Expected: equal or better performance due to 2 years of optimizations

**Alternatives considered:**
- Skip benchmarks: Would miss potential performance regressions
- Create new benchmarks: Existing suite is comprehensive and already established

## Risks / Trade-offs

**Risk:** Unexpected API incompatibility surfaces during testing
- **Mitigation:** Comprehensive test suite will catch issues early. Core API usage is conservative and well-documented as stable.

**Risk:** Transitive dependency conflicts with other project dependencies
- **Mitigation:** `go mod tidy` will resolve conflicts. If unresolvable, can constrain versions or update conflicting dependencies.

**Risk:** Performance regression in v2.1.4
- **Mitigation:** Benchmark suite provides quantitative comparison. If regression found, investigate Pebble configuration options or report upstream.

**Trade-off:** Users must handle their own format migration
- **Pro:** Keeps framework scope focused and simple
- **Con:** Users upgrading existing deployments need to understand format compatibility
- **Resolution:** Clear documentation in README with links to Pebble migration guide

**Trade-off:** Breaking change for framework users with v1.x data
- **Pro:** Adopts modern, actively maintained Pebble version
- **Con:** Users must plan format migration if upgrading existing data
- **Resolution:** This is acceptable for a framework; users have control over their upgrade timeline

## Migration Plan

Since this is a framework without production deployments:

1. **Update dependency:**
   - Modify go.mod to specify pebble v2.1.4
   - Run `go mod tidy` to resolve transitive dependencies

2. **Validation:**
   - Compile: `go build ./...`
   - Test: `go test ./...`
   - Benchmark: `go test -bench=. -benchmem`
   - Compare benchmark results

3. **Documentation:**
   - Update README.md with v2.x notes
   - Add format compatibility warning
   - Reference Pebble's migration documentation

4. **Rollback strategy:**
   - Git revert if issues found during validation
   - No data to rollback since framework has no persistent state
   - Users control their own upgrade timeline

## Open Questions

None. The upgrade path is straightforward given:
- Framework nature (no data migration)
- Conservative API usage (stable core operations only)
- Comprehensive test coverage
- Clear v2.x documentation from Pebble maintainers
