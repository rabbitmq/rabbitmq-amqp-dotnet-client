# Changelog

All notable changes to this project will be documented in this file.

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

