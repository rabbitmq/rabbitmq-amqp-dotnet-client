# Changelog

All notable changes to this project will be documented in this file.

## [[1.0.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v1.0.0)]

## 1.0.0 - 2026-20-04
- [Release 1.0.0](https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/releases/tag/v1.0.0)

### Highlights
- First stable 1.0 release.

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

