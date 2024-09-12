# RabbitMQ AMQP 1.0 .NET Client

This library is in early stages of development. It is meant to be used with RabbitMQ 4.0.

## How to Run

- Start the broker with `./.ci/ubuntu/one-node/gha-setup.sh start`. Note that this has been tested on Ubuntu 22 with docker.
- Run the tests with ` dotnet test ./Build.csproj  --logger "console;verbosity=detailed"`
- Stop RabbitMQ with `./.ci/ubuntu/one-node/gha-setup.sh stop`

## Getting Started

You can find an example in: `docs/Examples/GettingStarted`

## Install

The client is distributed via [NuGet](https://www.nuget.org/packages/RabbitMQ.AMQP.Client/).

## TODO

- [x] Declare queues
- [x] Declare exchanges
- [x] Declare bindings
- [x] Simple Publish messages
- [x] Implement backpressure (it is implemented with MaxInflightMessages `MaxInFlight(2000).`)
- [x] Simple Consume messages
- [x] Recovery connection on connection lost
- [x] Recovery management on connection lost
- [x] Recovery queues on connection lost
- [x] Recovery publishers on connection lost
- [x] Recovery consumers on connection lost
- [x] Implement Environment to manage the connections
- [x] Complete the consumer part with `pause` and `unpause`
- [x] Complete the binding/unbinding with the special characters
- [x] Complete the queues/exchanges name with the special characters
- [ ] Implement metrics ( See `System.Diagnostics.DiagnosticSource` [Link](https://learn.microsoft.com/en-us/dotnet/core/diagnostics/metrics-instrumentation) )
- [x] Recovery exchanges on connection lost
- [x] Recovery bindings on connection lost
- [ ] Docker image to test in LRE [not mandatory]
- [ ] Check the TODO in the code

