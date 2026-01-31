# Hashbox Project - GitHub Copilot Instructions

## Project Overview

Hashbox is a cross-platform, general-purpose data block storage system with an efficient backup client (Hashback) featuring full data de-duplication. The project is a derivative of a proprietary backup system (BUP) invented around 2001.

**Key features:**
- Variable-length block storage with optional compression
- Unique 128-bit hash-based block identification
- Cross-platform backup client with rolling checksum splitting
- Transactional dataset management with garbage collection
- Efficient incremental backups with local caching

**Target audience:** System administrators and users needing reliable, de-duplicated backup solutions.

**Project status:** Beta stage - not recommended for critical production use.

## Tech Stack

- **Language:** Go 1.25.6
- **Core libraries:**
  - `github.com/fredli74/bytearray` - Byte array handling
  - `github.com/fredli74/cmdparser` - Command-line parsing
  - `github.com/fredli74/lockfile` - File locking
- **Compression:** zlib (built-in)
- **Build system:** Shell scripts in `scripts/` directory
- **Testing:** Go's built-in testing framework
- **Containerization:** Docker with docker-compose support

## Project Structure

```
hashbox/
├── pkg/                - Shared packages
│   ├── core/          - Core library with block handling, protocol, and client code
│   ├── accountdb/     - Account database management
│   ├── storagedb/     - Storage database management
│   └── lockablefile/  - Lockable file utilities
├── server/            - Server implementation
├── hashback/          - Backup client with store, restore, and diff operations
├── util/              - Utility commands (hashbox-util)
├── scripts/           - Build and test scripts (build_all.sh, e2e_hashbox.sh, docker_*.sh)
├── docs/              - Documentation and setup guides
├── Dockerfile         - Container image definition
├── docker-compose.yml - Docker Compose configuration
└── .golangci.yml      - Linter configuration
```

## Coding Guidelines

### General Principles
- Follow standard Go conventions and idioms
- Use `go fmt` for consistent formatting (integrated into `build_all.sh`)
- Maintain cross-platform compatibility (Windows, macOS, Linux, FreeBSD)
- Write single binaries with minimal runtime dependencies
- Prioritize performance and efficiency for large-scale data operations

### Project Philosophy
1. **Follow first principles and design by contract** - Design interfaces with clear contracts and expectations
2. **Use asserts to catch incorrect usage** - Do not guard-rail invalid input outside the contract
3. **Panic on unexpected errors** - Only recover when errors are expected (rare)
4. **Optimize and reduce redundancies** - Split into functions if the same work repeats
5. **Test, iterate, and test again** - Continuously validate changes through testing
6. **Second-pass verification** - Ensure all code is needed; delete if possible
7. **When refactoring, compare logic** - Ensure behavior is unchanged; if changed, explain why
8. **Avoid aliases/duplicate helpers** - Prefer a single canonical API. Do not keep backward-compat shims unless strictly required, and remove them promptly after migration

### Code Organization
- Keep platform-specific code in separate files: `*_windows.go`, `*_unix.go`, and specific files like `hashback_mac.go` when additional platform differentiation is needed
- Use build tags when necessary for platform-specific functionality
- Minimize external dependencies (per project design goals)

### Error Handling
- Use Go's standard error handling patterns
- Log errors with appropriate context and timestamps
- Include connection/client identifiers in server logs for debugging

### Testing
- Write tests using Go's built-in testing framework
- Place test files alongside implementation: `*_test.go`
- Run tests with `go test -v` in respective directories
- Tests should pass on multiple platforms

### Naming Conventions
- Follow Go naming conventions (exported vs unexported identifiers)
- Use descriptive variable names for clarity
- Package names should be lowercase, single-word when possible

### Comments and Documentation
- Use the standard ASCII art header for copyright notices:
  ```go
  //	 ,+---+
  //	+---+´|    HASHBOX SOURCE
  //	| # | |    Copyright 2015-2024
  //	+---+´
  ```
- Write godoc-style comments for exported functions and types
- Document complex algorithms and data structures
- Include usage examples in command-line tools

### Performance Considerations
- Optimize for large file operations and network efficiency
- Use buffering appropriately for I/O operations
- Implement connection pooling where applicable
- Consider garbage collection impact in long-running processes

### Security
- Handle authentication securely (MD5 challenge-response currently used)
- Validate user input in command-line interfaces
- Sanitize file paths to prevent directory traversal
- Be mindful of data integrity and checksum validation

## Building and Testing

### Building the project
```bash
# Format and build all binaries for multiple platforms
./scripts/build_all.sh

# Build Docker image
./scripts/docker_build.sh
```

### Running tests
```bash
# Test per module (default approach)
cd pkg/core && go test ./...
cd pkg/accountdb && go test ./...
cd pkg/storagedb && go test ./...
cd pkg/lockablefile && go test ./...
cd server && go test ./...
cd hashback && go test ./...
cd util && go test ./...

# Or test a specific package
cd server && go test -v
```

### End-to-End Smoke Tests
```bash
# Run E2E smoke test from repository root (local mode)
./scripts/e2e_hashbox.sh

# Run E2E smoke test in Docker mode
E2E_DOCKER=1 ./scripts/e2e_hashbox.sh

# Environment variable overrides available:
# - E2E_SOURCE_DIR: Source directory for backup tests
# - E2E_DATASET: Dataset name for tests
# - E2E_TMP_ROOT: Temporary directory root
# See script header for full documentation
```

### Running the server
```bash
# Add a user
./hashbox-<platform> adduser <username> <password>

# Start the server
./hashbox-<platform> [-port=<port>] [-data=<path>] [-index=<path>] [-loglevel=<level>]

# Run garbage collection
./hashbox-<platform> gc [-compact] [-compact-only] [-force] [-threshold=<percentage>]
```

### Using the backup client
```bash
# Setup connection
./hashback -user=<username> -password=<password> -server=<ip>:<port> -progress -saveoptions

# Create a backup
./hashback -retaindays=7 -retainweeks=10 store <dataset> <folder|file>...

# List datasets or files
./hashback list <dataset> [<backup id>|.] ["<path>"]

# Restore files
./hashback restore <dataset> <backup id>|. ["<path>"...] <dest-folder>
```

## Additional Resources

- **README:** [README.md](/README.md) - Main project documentation
- **Documentation:** [docs/](/docs/) - Setup guides and specifications
- **License:** MIT License (see [LICENSE](/LICENSE))
- **Repository:** https://github.com/fredli74/hashbox
- **Build Status:** https://semaphoreci.com/fredli74/hashbox

## Development Roadmap

When working on new features, be aware of planned improvements:
- Platform-specific file information storage
- Server admin interface (API)
- Scheduled/triggered garbage collection
- Online GC mark and sweep
- Server mirroring
- Client GUI
- Data encryption
- Quota calculations and restrictions

## Notes for AI Assistance

- Always test changes across platforms if modifying platform-specific code
- Verify backward compatibility with existing data stores when changing storage formats
- Consider the impact on existing backups when modifying the protocol
- Maintain the single-binary philosophy - avoid adding heavy dependencies
- Keep the permissive MIT License in mind when suggesting new dependencies
