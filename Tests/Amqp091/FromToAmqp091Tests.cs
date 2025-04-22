// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
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
            _queueName = "q";

            IQueueSpecification queueSpec = _management.Queue().Name(_queueName).Type(QueueType.QUORUM);
            await queueSpec.DeclareAsync();

            var publisher = await _connection.PublisherBuilder().BuildAsync();
            byte[] body = System.Text.Encoding.UTF8.GetBytes("{Text:as,Seq:1,Max:7000}");
            IMessage amqpMessage = new AmqpMessage(body).ToAddress().Queue(_queueName).Build();
            for (int i = 0; i < 1; i++)
            {
                PublishResult result = await publisher.PublishAsync(message: amqpMessage).ConfigureAwait(true);
                Assert.NotNull(result);
                Assert.Equal(OutcomeState.Accepted, result.Outcome.State);
            }


            var factory = new ConnectionFactory();
            var connection = await factory.CreateConnectionAsync();
            var channel = await connection.CreateChannelAsync();
            var consumer091 = new AsyncEventingBasicConsumer(channel);
            var tcs091 = new TaskCompletionSource<BasicDeliverEventArgs>();
            consumer091.ReceivedAsync += async (sender, ea) =>
            {
                tcs091.SetResult(ea);
                await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
            };
            await channel.BasicConsumeAsync(_queueName, false, "consumerTag", consumer091);
            var receivedMessage091 = await tcs091.Task;
            Assert.NotNull(receivedMessage091);
            Assert.Equal(_queueName, receivedMessage091.RoutingKey);
            Assert.Equal("consumerTag", receivedMessage091.ConsumerTag);
            Assert.Equal("{Text:as,Seq:1,Max:7000}",
                System.Text.Encoding.UTF8.GetString(receivedMessage091.Body.ToArray()));
        }
    }
}
