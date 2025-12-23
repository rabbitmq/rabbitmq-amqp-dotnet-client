AMQP 1.0 over WebSocket Example
===============================================================

This example demonstrates how to use AMQP 1.0 over WebSocket. </br> 
You need [Tanzu RabbitMQ 4.0](https://www.vmware.com/products/app-platform/tanzu-rabbitmq) or later with the AMQP 1.0 and `rabbitmq_web_amqp` plugins enabled.

For more info read the blog post: [AMQP 1.0 over WebSocket](https://rabbitmq.com/blog/2024/using-websockets-with-rabbitmq-and-amqp-1-0/)

To run the example you need to have:
- Tanzu RabbitMQ 4.0 or later with the AMQP 1.0 and `rabbitmq_web_amqp` plugins enabled.
- A vhost called `ws` configured for WebSocket connections.
- A user `rabbit` pwd `rabbit` with access to the `ws` vhost.
