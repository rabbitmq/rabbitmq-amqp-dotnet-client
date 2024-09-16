# RabbitMQ AMQP 1.0 .NET Client

This library is meant to be used with RabbitMQ 4.0.
Still work in progress suitable for testing in pre-production environments

## How to Run

- Start the broker with `./.ci/ubuntu/one-node/gha-setup.sh start`. Note that this has been tested on Ubuntu 22 with docker.
- Run the tests with ` dotnet test ./Build.csproj  --logger "console;verbosity=detailed"`
- Stop RabbitMQ with `./.ci/ubuntu/one-node/gha-setup.sh stop`

## Getting Started

You can find an example in: `docs/Examples/GettingStarted`

## Install

The client is distributed via [NuGet](https://www.nuget.org/packages/RabbitMQ.AMQP.Client/).

## Documentation

- [API](https://rabbitmq.github.io/rabbitmq-amqp-dotnet-client/api/RabbitMQ.AMQP.Client.html)
