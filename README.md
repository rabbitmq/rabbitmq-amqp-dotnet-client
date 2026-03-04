# RabbitMQ AMQP 1.0 .NET Client

This library is meant to be used with RabbitMQ `4.x`.


## Install

The client is distributed via [NuGet](https://www.nuget.org/packages/RabbitMQ.AMQP.Client/).

## Examples

Inside the [docs/Examples](./docs/Examples) directory you can find examples of how to use the client.


## Documentation

- [Client Guide](https://www.rabbitmq.com/client-libraries/amqp-client-libraries)
- [API](https://rabbitmq.github.io/rabbitmq-amqp-dotnet-client/api/RabbitMQ.AMQP.Client.html)


## How to Run

- Start single node with `make rabbitmq-server-start`. Note that this has been tested on Ubuntu 22 with docker.
- Start cluster three nodes with `make rabbitmq-cluster-start`. Note that this has been tested on Ubuntu 22 with docker.
- Run the tests with ` make test`
- Running the cluster covers more features 
