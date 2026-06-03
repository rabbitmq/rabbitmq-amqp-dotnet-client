# Agent Guide

This document provides essential information for AI agents working on the RabbitMQ AMQP 1.0 .NET Client codebase.

## Project Overview

This is a .NET client library for RabbitMQ that implements the AMQP 1.0 protocol. The library is designed to work with RabbitMQ 4.x and provides a high-level API for connecting to RabbitMQ, publishing messages, consuming messages, and managing RabbitMQ entities.

Current version: **0.51.0**

## Key Directories

### `RabbitMQ.AMQP.Client/`
The main library code:
- **Public interfaces**: `IEnvironment`, `IConnection`, `IPublisher`, `IConsumer`, `IRequester`, `IResponder`, `IManagement`, `ILifeCycle`, `IRecoveryConfiguration`, `IBackOffDelayPolicy`, `ITopologyListener`, `IMetricsReporter`, etc.
- **Implementation classes**: Located in `Impl/` subdirectory (e.g., `AmqpConnection`, `AmqpConsumer`, `AmqpPublisher`, `AmqpEnvironment`)
- **Core abstractions**: Interfaces define the public API, implementations are in `Impl/`
- **Other notable files**: `Affinity.cs`, `ConnectionSettings.cs`, `SaslMechanism.cs`, `ByteCapacity.cs`, `Extensions.cs`, `Utils.cs`, `FeatureFlags.cs`, `Consts.cs`

### `RabbitMQ.AMQP.Client/Impl/`
All implementation classes:
- `AbstractLifeCycle.cs` — Base lifecycle class; also contains `AbstractReconnectLifeCycle` with back-off reconnect logic
- `AmqpEnvironment.cs` — `IEnvironment` implementation; manages a pool of connections
- `AmqpConnection.cs` — Main connection implementation
- `AmqpConsumer.cs` / `AmqpConsumerBuilder.cs` — Consumer and its builder
- `AmqpPublisher.cs` / `AmqpPublisherBuilder.cs` — Publisher and its builder
- `AmqpManagement.cs` / `AmqpManagementParameters.cs` — Management API
- `AmqpRequester.cs` / `AmqpResponder.cs` — RPC-style request/response
- `AmqpSessionManagement.cs` — AMQP session handling
- `AmqpMessage.cs` — Message implementation
- `AmqpQueueSpecification.cs`, `AmqpExchangeSpecification.cs`, `AmqpBindingSpecification.cs` — Entity specs
- `RecordingTopologyListener.cs` — Records topology for recovery
- `UnsettledMessageCounter.cs` — Tracks unsettled messages
- `AddressBuilder.cs`, `Visitor.cs`, `DeliveryContext.cs`, `BindingSpec.cs`, `QueueSpec.cs`, `ExchangeSpec.cs`

### `Tests/`
Test suite using xUnit:
- **Top-level tests**: `AmqpTests.cs`, `AnonymousPublisherTests.cs`, `BindingsTests.cs`, `ByteCapacityTests.cs`, `ClusterTests.cs`, `ConnectionRecoveryTests.cs`, `EnvironmentTests.cs`, `MessagesTests.cs`, `MetricsTests.cs`, `OAuth2Tests.cs`, `TlsConnectionTests.cs`, `UtilsTests.cs`, `AddressBuilderTests.cs`
- **Subdirectories by feature area**:
  - `Affinity/` — Node affinity tests
  - `Amqp091/` — AMQP 0-9-1 compatibility tests
  - `ConnectionTests/` — `ConnectionTests.cs`, `ConnectionSettingsTests.cs`, `SaslConnectionTests.cs`
  - `Consumer/` — `BasicConsumerTests.cs`, `ConsumerDispositionTests.cs`, `ConsumerOutcomeTests.cs`, `ConsumerPauseTests.cs`, `ConsumerSqlFilterTests.cs`, `PreSettledConsumerTests.cs`, `StreamConsumerTests.cs`
  - `DirectReply/` — Direct reply-to tests
  - `Management/` — `ManagementTests.cs`, `MockManagementTests.cs`
  - `Publisher/` — `PublisherTests.cs`
  - `Recovery/` — `PublisherConsumerRecoveryTests.cs`, `CustomPublisherConsumerRecoveryTests.cs`
  - `RequesterResponser/` — RPC tests
  - `Sessions/` — Session management tests
- `IntegrationTest.cs` / `IntegrationTest.Static.cs` — Common test infrastructure
- `HttpApiClient.cs` — HTTP management API client used in tests

### `docs/Examples/`
Example code demonstrating library usage:
- `GettingStarted`
- `Affinity`
- `BatchDispositions`
- `ConsumerTimeout` (quorum queue `x-consumer-timeout`)
- `HAClient`
- `OAuth2`
- `OpenTelemetryIntegration`
- `PerformanceTest`
- `Presettled`
- `Rpc` (Requester/Responder)
- `StreamFilter`
- `WebSockets`

## Architecture Patterns

### Interface-Based Design
- Public API is defined through interfaces (e.g., `IConnection`, `IPublisher`, `IConsumer`)
- Implementations are in the `Impl/` namespace
- `AmqpEnvironment` is the only public entry point; use `AmqpEnvironment.Create(connectionSettings)` to bootstrap

### Builder Pattern
Builders are used for constructing complex objects:
- `IPublisherBuilder` → `IPublisher`
- `IConsumerBuilder` → `IConsumer`
- `IRequesterBuilder` → `IRequester`
- `IResponderBuilder` → `IResponder`
- `ConnectionSettingsBuilder` → `ConnectionSettings`

### Lifecycle Management
- Most entities implement `ILifeCycle` (extends `IDisposable`)
- States: `Open`, `Reconnecting`, `Closing`, `Closed`
- Entities expose `CloseAsync()` and a `ChangeState` event (`LifeCycleCallBack` delegate)
- `AbstractLifeCycle` is the base class; `AbstractReconnectLifeCycle` adds back-off reconnect logic
- `AmqpNotOpenException` is thrown when an operation is attempted on a non-open resource

### Recovery / Reconnection
- Configured via `IRecoveryConfiguration` / `RecoveryConfiguration`
- `Activated(bool)` — enable/disable reconnect (default: enabled)
- `Topology(bool)` — enable/disable topology recovery after reconnect (default: disabled)
- `BackOffDelayPolicy(IBackOffDelayPolicy)` — customise delay between reconnect attempts
- `RecordingTopologyListener` records declared entities for topology recovery

### Node Affinity
- `IAffinity` / `DefaultAffinity` in `Affinity.cs` — connect to the node that owns a specific queue
- `Operation.Publish` targets the queue leader; `Operation.Consume` targets a follower
- `AffinityUtils.TryToFindUriNode` iterates connections until the correct node is found
- Configured via `ConnectionSettings.Affinity`

### Consumer Settle Strategies (`ConsumerSettleStrategy`)
- `ExplicitSettle` — default; messages must be settled manually via `IContext`
- `PreSettled` — messages are auto-settled on receipt (no redelivery on failure)
- `DirectReplyTo` — enables direct reply-to consumer (pre-settled by default)
- Set via `IConsumerBuilder.SettleStrategy(ConsumerSettleStrategy...)`

### Async/Await Pattern
- All I/O operations are asynchronous
- Methods return `Task` or `Task<T>`
- Use `async`/`await` throughout the codebase

## Code Conventions

### Naming
- Interfaces start with `I` (e.g., `IConnection`, `IPublisher`)
- Implementation classes are prefixed with `Amqp` (e.g., `AmqpConnection`, `AmqpPublisher`)
- Private fields use `_camelCase`
- Public properties use `PascalCase`

### File Organization
- One public interface/class per file
- Implementation classes in `Impl/` subdirectory
- File names match class/interface names

### Error Handling
- Custom exceptions: `ConnectionException`, `ManagementException`, `PublisherException`, `ConsumerException`
- `AmqpNotOpenException` — thrown when operating on a closed/closing/reconnecting resource
- `InternalBugException` — for internal errors that should never occur

### Thread Safety
- `IEnvironment` instances are expected to be thread-safe
- `AmqpEnvironment` uses `ConcurrentDictionary` for connection tracking and `Interlocked` for IDs
- Connection instances should handle concurrent operations safely

## Important Files

### Build Configuration
- `Directory.Build.props` — Common build properties; treats warnings as errors (`TreatWarningsAsErrors=true`)
- `Directory.Packages.props` — Centralized package version management
- `global.json` — .NET SDK version specification
- `Build.csproj` — Main build/test project
- `Makefile` — Convenience targets

### Documentation
- `CHANGELOG.md` — Release notes and changes
- `README.md` — Project overview and quick start
- `PublicAPI.Shipped.txt` / `PublicAPI.Unshipped.txt` — API surface tracking

### Key Implementation Files
- `Impl/AmqpEnvironment.cs` — Entry point; manages connections
- `Impl/AmqpConnection.cs` — Main connection implementation
- `Impl/AmqpConsumer.cs` — Consumer implementation
- `Impl/AmqpPublisher.cs` — Publisher implementation
- `Impl/AmqpManagement.cs` — Management API implementation
- `Impl/AbstractLifeCycle.cs` — Base lifecycle and reconnect logic
- `Impl/RecordingTopologyListener.cs` — Topology recovery support
- `ConnectionSettings.cs` — Connection configuration
- `Affinity.cs` — Node affinity logic
- `IRecoveryConfiguration.cs` — Recovery configuration interface and default implementation
- `FeatureFlags.cs` — Feature flag definitions

## Testing

### Running Tests
```bash
make test
```

### Test Infrastructure
- Tests require a running RabbitMQ instance
- Use `make rabbitmq-cluster-start` to start cluster broker
- Use `make rabbitmq-server-start` to start single node
- `IntegrationTest.cs` provides common test utilities
- `HttpApiClient.cs` wraps the RabbitMQ HTTP management API for test assertions

### Test Organization
- Tests are organized by feature area (Consumer, Publisher, Management, Recovery, etc.)
- Integration tests verify end-to-end functionality
- Some tests use mocks (e.g., `MockManagementTests.cs`)

## Development Workflow

### Building
```bash
make
```

Build configuration is managed through `Directory.Build.props`.

### Code Quality
- Warnings are treated as errors (`TreatWarningsAsErrors` is `true`)
- Public API changes must be tracked in `PublicAPI.Unshipped.txt`

### Versioning
- Version information is managed in project files
- Releases are tagged in git (e.g., `v0.51.0`)
- Changelog entries follow a specific format (see `CHANGELOG.md`)

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
4. Update `CHANGELOG.md` under the appropriate section (Fix, Changed, etc.)

## Dependencies

### External Libraries
- **Microsoft AMQP.Net Lite** — Core AMQP protocol implementation
- **xUnit** — Testing framework
- Other dependencies managed in `Directory.Packages.props`

## Important Notes

- The library uses AMQP 1.0 protocol (not AMQP 0-9-1)
- Designed for RabbitMQ 4.x
- All operations are asynchronous
- Thread-safety is important for `IEnvironment` and connection instances
- The library supports:
  - WebSockets (added in v0.50.0)
  - OAuth2 authentication
  - TLS connections
  - Direct reply-to
  - Connection recovery with topology replay
  - Streams (with offset, filter, and SQL filter expression support — SQL filters require RabbitMQ 4.2+)
  - Node affinity (publish to leader, consume from follower)
  - Pre-settled consumers (added in v0.51.0)
  - OpenTelemetry metrics integration
  - SASL authentication mechanisms

## Resources

- [Client Guide](https://www.rabbitmq.com/client-libraries/amqp-client-libraries)
- [API Documentation](https://rabbitmq.github.io/rabbitmq-amqp-dotnet-client/api/RabbitMQ.AMQP.Client.html)
- [GitHub Repository](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client)
- [NuGet Package](https://www.nuget.org/packages/RabbitMQ.AMQP.Client/)
