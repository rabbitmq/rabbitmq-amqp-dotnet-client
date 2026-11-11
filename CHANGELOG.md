# Changelog

All notable changes to this project will be documented in this file.

## [Unreleased]

### Enhancements
- Support consumer priority via `IConsumerBuilder.Priority(int)` (AMQP 1.0 attach property `rabbitmq:priority`). Consumers with a higher priority receive messages first. See [#183](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues/183)
- Support `x-stream-initial-offset` for stream queues via `IStreamSpecification.InitialOffset(long)` (requires RabbitMQ 4.4+). Adds `FeatureFlags.Is44OrMore` and `FeatureFlags.IsStreamInitialOffsetSupported`. Ported from [rabbitmq-amqp-go-client#128](https://github.com/rabbitmq/rabbitmq-amqp-go-client/pull/128)

## [[1.4.1](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v1.4.1)]

## 1.4.1 - 2026-19-10
- [Release 1.4.1](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v1.4.1)

### Fix
- Apply consumer quorum and JMS options without `Builder()` by @Demivan in [#187](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/187)
- Check consumer timeout support for `OnDeliveryRelease` without SAC by @Demivan in [#188](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/188)

### Contributors
- @Demivan made their first contribution in [#187](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/187)


## [[1.4.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v1.4.0)]

## 1.4.0 - 2026-09-09
- [Release 1.4.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v1.4.0)

### Enhancements
- Add consumer timeout support for quorum queues in the JMS client by @Gsantomaggio in [#169](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/169)
- Add `DelayedQueue` queue type support by @Gsantomaggio in [#178](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/178)
- Allow the application to supply the connection transport by @VadimVolue in [#180](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/180)

### Fix
- `InitTlsSettings` preserves existing TlsSettings by @philipfalkner in [#176](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/176)

### Changed
- Bump Azure dependency by @Gsantomaggio in [#182](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/182)

### Contributors
- @philipfalkner made their first contribution in [#176](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/176)
- @VadimVolue made their first contribution in [#180](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/180)


## [[1.3.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v1.3.0)]

## 1.3.0 - 2026-20-07
- [Release 1.3.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v1.3.0)

### Enhancements
- Handle invalid server version responses by @MirahImage in [#170](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/170)
- Handle management errors by @MirahImage in [#171](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/171)
- Add `IContext.DelayedRetry()` and `DelayedRetry(TimeSpan)` for quorum queues by @Gsantomaggio in [#172](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/172)

### Fix
- Correct delayed-retry documentation inaccuracies by @Gsantomaggio in [#174](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/174)


## [[1.2.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v1.2.0)]

## 1.2.0 - 2026-15-06
- [Release 1.2.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v1.2.0)

### Enhancements
- Support quorum queue delayed retry (RabbitMQ 4.3+) by @Gsantomaggio in [#168](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/168)


## [[1.1.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v1.1.0)]

## 1.1.0 - 2026-18-05
- [Release 1.1.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v1.1.0)

### Enhancements
- Support `rejected-by` and rejection reason (RabbitMQ 4.3+) by @Gsantomaggio in [#167](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/167)

### Fix
- Preserve broker URIs in `ConnectionSettingsBuilder.From` by @Gsantomaggio in [#165](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/165)


## [[1.0.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v1.0.0)]

## 1.0.0 - 2026-20-04
- [Release 1.0.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v1.0.0)

### Highlights
- First stable 1.0 release.

### Fix
- `ConnectionSettingsBuilder.From` now copies cluster broker URIs in order (with correct virtual host path) instead of rebuilding from resolved addresses, and preserves WebSocket broker URIs including the resource path.

### Changed
- Update AMQP.Net Lite to 2.5.1 by @Gsantomaggio in [#158](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/158)
- Improve examples by @Gsantomaggio in [#162](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/162)

### Breaking changes
- Quorum single-active-consumer state notifications are registered with `Quorum().SingleActiveConsumerStateChanged(...)` on the consumer builder by @Gsantomaggio in [#161](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/161). Code that used the previous top-level consumer builder hook (see 0.60.0 notes) must chain `.Quorum()` before setting the handler.

### Documentation
- Group stream filtering documentation by group id by @Gsantomaggio in [#160](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/160)

### Project
- Add `.github/release.yml` for release changelog generation by @Gsantomaggio in [#156](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/156)


## [[0.60.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v0.60.0)]

## 0.60.0 - 2026-17-03
- [Release 0.60.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v0.60.0)

### Added
- Quorum single-active-consumer state notifications via AMQP 1.0 FLOW link-state (`rabbitmq:active`): `IConsumerBuilder.SingleActiveConsumerStateChanged` (requires RabbitMQ 4.3+ and a quorum queue from `Queue(IQueueSpecification)`).
- Implement connection affinity feature by @Gsantomaggio in [#149](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/149)
- Add cancellation token to the CreateConnectionAsync calls by @Gsantomaggio in [#151](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/151)
- Add SingleActiveConsumerTest for IQueueSpecification.SingleActiveConsumer by @Gsantomaggio in [#153](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/153)
- Example single active consumer by @Gsantomaggio in [#154](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/154)
- Document AmqpConsumer and stress handler safety by @Gsantomaggio in [#155](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/155)

### Fix
- Use TotalSeconds to set x-max-age by @pterygota in [#148](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/148)

### Contributors
- @pterygota made their first contribution in [#148](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/148)


## [[0.51.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v0.51.0)]

## 0.51.0 - 2026-16-02
- [Release 0.51.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v0.51.0)

### Added
- Implement PreSettled consumer feature by @Gsantomaggio in [#143](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/143)

### Changed
- Change consumer options to unify all the AMQP 1.0 clients' interfaces by @Gsantomaggio in [#144](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/144)
- Remove default value by @Gsantomaggio in [#145](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/145)

### Breaking changes
- Minor breaking change in [#144](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/144): consumer options unified across all AMQP 1.0 clients. Use `SettleStrategy(ConsumerSettleStrategy....)` on the consumer builder to define the settle strategy:

```csharp
IConsumer consumer = await connection.ConsumerBuilder()
    .SettleStrategy(ConsumerSettleStrategy....) // define the Settle Strategy
    .BuildAsync();
```


## [[0.50.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v0.50.0)]

## 0.50.0 - 2026-14-01
- [Release 0.50.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v0.50.0)

### Added
- Support AMQP over WebSockets by @Luka-He in [#141](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/141)

### Contributors
- @Luka-He made their first contribution in [#141](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/141)


## [[0.4.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v0.4.0)]

## 0.4.0 - 2025-18-11
- [Release 0.4.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v0.4.0)

### Added
- Implement direct reply to feature by @Gsantomaggio in [#135](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/135)

### Changed
- Update the request and response example by @Gsantomaggio in [#136](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/136)

### Breaking changes
- Rename RpcClient to Requester and RpcServer to Responder by @Gsantomaggio in [#135](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/135)


## 0.4.1 - 2025-16-12
- [Release 0.4.1](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v0.4.1)

### Fix
- Fix multi-tread creation session in [#139](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/pull/139)

