// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Amqp091
{
    public class FromToAmqp091Tests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
    {
        [Fact]
        public async Task ToAmqp091()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            IQueueSpecification queueSpec = _management.Queue().Name(_queueName).Type(QueueType.QUORUM);
            await queueSpec.DeclareAsync();

            var publisher = await _connection.PublisherBuilder().BuildAsync();
            const string body = "{Text:as,Seq:1,Max:7000}";
            IMessage amqpMessage = new AmqpMessage(body).ToAddress().Queue(_queueName).Build();
            for (int i = 0; i < 1; i++)
            {
                PublishResult result = await publisher.PublishAsync(message: amqpMessage).ConfigureAwait(true);
                Assert.NotNull(result);
                Assert.Equal(OutcomeState.Accepted, result.Outcome.State);
            }

            var factory = new ConnectionFactory();
            var connection = factory.CreateConnection();
            var channel = connection.CreateModel();
            var consumer091 = new EventingBasicConsumer(channel);
            var tcs091 = new TaskCompletionSource<BasicDeliverEventArgs>();
            consumer091.Received += (sender, ea) =>
            {
                tcs091.SetResult(ea);
                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            };
            channel.BasicConsume(_queueName, false, "consumerTag", consumer091);
            var receivedMessage091 = await tcs091.Task;
            Assert.NotNull(receivedMessage091);
            Assert.Equal(_queueName, receivedMessage091.RoutingKey);
            Assert.Equal("consumerTag", receivedMessage091.ConsumerTag);
            Assert.Equal("{Text:as,Seq:1,Max:7000}",
                System.Text.Encoding.UTF8.GetString(receivedMessage091.Body.ToArray()));
            channel.Close();
            connection.Close();
        }

        [Fact]
        public async Task FromAmqp091()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            IQueueSpecification queueSpec = _management.Queue().Name(_queueName).Type(QueueType.QUORUM);
            await queueSpec.DeclareAsync();

            // publish a message with AMQP 0-9-1
            var factory = new ConnectionFactory();
            var connection = factory.CreateConnection();
            var channel = connection.CreateModel();
            channel.BasicPublish(
                exchange: "",
                routingKey: _queueName,
                basicProperties: null,
                body: System.Text.Encoding.UTF8.GetBytes("{Text:as,Seq:1,Max:7000}"));

            TaskCompletionSource<IMessage> tcs = new();
            IConsumer consumer = await _connection.ConsumerBuilder()
                .Queue(_queueName)
                .MessageHandler((context, message) =>
                {
                    tcs.SetResult(message);
                    context.Accept();
                    return Task.CompletedTask;
                }).BuildAndStartAsync();

            var receivedMessage = await tcs.Task;
            Assert.NotNull(receivedMessage);
            Assert.Equal("{Text:as,Seq:1,Max:7000}", receivedMessage.BodyAsString());
            channel.Close();
            connection.Close();
            await consumer.CloseAsync();
        }
    }
}
