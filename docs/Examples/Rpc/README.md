## Request and Response Example

This example demonstrates how to create a simple RPC (Remote Procedure Call) 
Service will allow clients (or requesters) to send a request with a message and receive a response from the server 
(or responder) with a greeting.

This example uses [Direct Reply-To feature](https://www.rabbitmq.com/docs/next/direct-reply-to#overview)
since the reply queue is not specified by the client, but it is automatically created by the server.
