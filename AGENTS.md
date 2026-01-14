# Agent Guide

This document provides essential information for AI agents working on the RabbitMQ AMQP 1.0 .NET Client codebase.

## Project Overview

This is a .NET client library for RabbitMQ that implements the AMQP 1.0 protocol. The library is designed to work with RabbitMQ 4.x and provides a high-level API for connecting to RabbitMQ, publishing messages, consuming messages, and managing RabbitMQ entities.

## Key Directories

### `RabbitMQ.AMQP.Client/`
The main library code:
- **Public interfaces**: `IEnvironment`, `IConnection`, `IPublisher`, `IConsumer`, `IRequester`, `IResponder`, `IManagement`, etc.
- **Implementation classes**: Located in `Impl/` subdirectory (e.g., `AmqpConnection`, `AmqpConsumer`, `AmqpPublisher`)
- **Core abstractions**: Interfaces define the public API, implementations are in `Impl/`

### `Tests/`
Test suite using xUnit:
- Integration tests for various features
- Test categories: Consumer, Publisher, Management, Recovery, Sessions, etc.
- Uses `IntegrationTest.cs` for common test infrastructure

### `docs/Examples/`
Example code demonstrating library usage:
- GettingStarted
- BatchDispositions
- HAClient
- OAuth2
- OpenTelemetryIntegration
- PerformanceTest
- Rpc (Requester/Responder)
- StreamFilter
- WebSockets

## Architecture Patterns

### Interface-Based Design
- Public API is defined through interfaces (e.g., `IConnection`, `IPublisher`, `IConsumer`)
- Implementations are in the `Impl/` namespace and are internal
- This allows for easier testing and future extensibility

### Builder Pattern
- Builders are used for constructing complex objects:
  - `IPublisherBuilder` → `IPublisher`
  - `IConsumerBuilder` → `IConsumer`
  - `IRequesterBuilder` → `IRequester`
  - `IResponderBuilder` → `IResponder`

### Lifecycle Management
- Most entities implement `ILifeCycle` interface
- States: `Open`, `Reconnecting`, `Closing`, `Closed`
- Entities have `CloseAsync()` method and `ChangeState` event

### Async/Await Pattern
- All I/O operations are asynchronous
- Methods return `Task` or `Task<T>`
- Use `async`/`await` throughout the codebase

## Code Conventions

### Naming
- Interfaces start with `I` (e.g., `IConnection`, `IPublisher`)
- Implementation classes are prefixed with `Amqp` (e.g., `AmqpConnection`, `AmqpPublisher`)
- Private fields use camelCase
- Public properties use PascalCase

### File Organization
- One public interface/class per file
- Implementation classes in `Impl/` subdirectory
- File names match class/interface names

### Error Handling
- Custom exceptions: `ConnectionExceptions`, `ManagementExceptions`, `PublisherException`, `ConsumerException`
- `InternalBugException` for internal errors that should not occur

### Thread Safety
- `IEnvironment` instances are expected to be thread-safe
- Connection instances should handle concurrent operations safely

## Important Files

### Build Configuration
- `Directory.Build.props`: Common build properties, treats warnings as errors
- `Directory.Packages.props`: Centralized package version management
- `global.json`: .NET SDK version specification
- `Build.csproj`: Main build/test project

### Documentation
- `CHANGELOG.md`: Release notes and changes
- `README.md`: Project overview and quick start
- `PublicAPI.Shipped.txt` / `PublicAPI.Unshipped.txt`: API surface tracking

### Key Implementation Files
- `Impl/AmqpConnection.cs`: Main connection implementation
- `Impl/AmqpConsumer.cs`: Consumer implementation
- `Impl/AmqpPublisher.cs`: Publisher implementation
- `Impl/AmqpManagement.cs`: Management API implementation
- `ConnectionSettings.cs`: Connection configuration
- `FeatureFlags.cs`: Feature flag definitions

## Testing

### Running Tests
```bash
dotnet test ./Build.csproj --logger "console;verbosity=detailed"
```

### Test Infrastructure
- Tests require a running RabbitMQ instance
- Use `./.ci/ubuntu/one-node/gha-setup.sh start` to start broker
- Use `./.ci/ubuntu/one-node/gha-setup.sh stop` to stop broker
- `IntegrationTest.cs` provides common test utilities

### Test Organization
- Tests are organized by feature area (Consumer, Publisher, Management, etc.)
- Integration tests verify end-to-end functionality
- Some tests use mocks (e.g., `MockManagementTests.cs`)

## Development Workflow

### Building
- Use `dotnet build` or `dotnet build ./Build.csproj`
- Build configuration is managed through `Directory.Build.props`

### Code Quality
- Warnings are treated as errors (`TreatWarningsAsErrors` is `true`)
- Public API changes should be tracked in `PublicAPI.Unshipped.txt`

### Versioning
- Version information is managed in project files
- Releases are tagged in git (e.g., `v0.50.0`)
- Changelog entries follow a specific format

## Common Tasks

### Adding a New Feature
1. Define public interface(s) in `RabbitMQ.AMQP.Client/`
2. Implement in `RabbitMQ.AMQP.Client/Impl/`
3. Add tests in `Tests/`
4. Update `CHANGELOG.md`
5. Update `PublicAPI.Unshipped.txt` if adding public APIs
6. Add example in `docs/Examples/` if applicable

### Modifying Existing Features
1. Check if changes affect public API (update `PublicAPI.Unshipped.txt` if so)
2. Update implementation in `Impl/`
3. Update/add tests
4. Update `CHANGELOG.md` if user-facing

### Fixing Bugs
1. Add test case that reproduces the bug
2. Fix the implementation
3. Verify test passes
4. Update `CHANGELOG.md` under appropriate section (Fix, Changed, etc.)

## Dependencies

### External Libraries
- Microsoft AMQP.Net Lite: Core AMQP protocol implementation
- xUnit: Testing framework
- Other dependencies managed in `Directory.Packages.props`

## Important Notes

- The library uses AMQP 1.0 protocol (not AMQP 0-9-1)
- Designed for RabbitMQ 4.x
- All operations are asynchronous
- Thread-safety is important for `IEnvironment` and connection instances
- The library supports features like:
  - WebSockets (added in v0.50.0)
  - OAuth2 authentication
  - Direct reply-to
  - Connection recovery
  - Streams

## Resources

- [Client Guide](https://www.rabbitmq.com/client-libraries/amqp-client-libraries)
- [API Documentation](https://rabbitmq.github.io/rabbitmq-amqp-dotnet-client/api/RabbitMQ.AMQP.Client.html)
- [GitHub Repository](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client)
- [NuGet Package](https://www.nuget.org/packages/RabbitMQ.AMQP.Client/)
