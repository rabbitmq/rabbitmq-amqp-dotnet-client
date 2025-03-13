### OAuth2 Example

This example demonstrates how to use the OAuth2 authentication mechanism.

It is meant to be used with the RabbitMQ server configured for the CI.

To run the example:
 - make rabbitmq-server-start
 - dotnet run


The example will create a connection given the token and refresh the token every 4 seconds.

