// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using Xunit;
using Xunit.Abstractions;

namespace Tests
{
    public class SingleActiveConsumerTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
    {
        [Fact]
        public async Task DeclareQueueWithSingleActiveConsumerTrue()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            IQueueSpecification queueSpec = _management.Queue().Name(_queueName).SingleActiveConsumer(true);
            IQueueInfo queueInfo = await queueSpec.DeclareAsync();

            Assert.Equal(_queueName, queueInfo.Name());
            Assert.True(queueInfo.Arguments().ContainsKey("x-single-active-consumer"));
            Assert.Equal(true, queueInfo.Arguments()["x-single-active-consumer"]);

            IQueueInfo queueInfoFromGet = await _management.GetQueueInfoAsync(queueSpec);
            Assert.True(queueInfoFromGet.Arguments().ContainsKey("x-single-active-consumer"));
            Assert.Equal(true, queueInfoFromGet.Arguments()["x-single-active-consumer"]);
        }

        [Fact]
        public async Task DeclareQueueWithSingleActiveConsumerFalse()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            IQueueSpecification queueSpec = _management.Queue().Name(_queueName).SingleActiveConsumer(false);
            IQueueInfo queueInfo = await queueSpec.DeclareAsync();

            Assert.Equal(_queueName, queueInfo.Name());
            Assert.True(queueInfo.Arguments().ContainsKey("x-single-active-consumer"));
            Assert.Equal(false, queueInfo.Arguments()["x-single-active-consumer"]);
        }

        [Fact]
        public async Task SingleActiveConsumerFluentChainWithQuorumQueue()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            IQueueSpecification queueSpec = _management.Queue()
                .Name(_queueName)
                .Type(QueueType.QUORUM)
                .SingleActiveConsumer(true);
            IQueueInfo queueInfo = await queueSpec.DeclareAsync();

            Assert.Equal(_queueName, queueInfo.Name());
            Assert.Equal(QueueType.QUORUM, queueInfo.Type());
            Assert.True(queueInfo.Arguments().ContainsKey("x-single-active-consumer"));
            Assert.Equal(true, queueInfo.Arguments()["x-single-active-consumer"]);
        }

        [Fact]
        public async Task PublishAndConsumeWithSingleActiveConsumerQueue()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            IQueueSpecification queueSpec = _management.Queue().Name(_queueName).SingleActiveConsumer(true);
            await queueSpec.DeclareAsync();

            await PublishAsync(queueSpec, 1);

            TaskCompletionSource<IMessage> tcs = CreateTaskCompletionSource<IMessage>();
            IConsumer consumer = await _connection.ConsumerBuilder()
                .Queue(queueSpec)
                .MessageHandler((context, message) =>
                {
                    context.Accept();
                    tcs.SetResult(message);
                    return Task.CompletedTask;
                })
                .BuildAndStartAsync();

            await WhenTcsCompletes(tcs);
            IMessage receivedMessage = await tcs.Task;
            Assert.Equal("message_0", receivedMessage.BodyAsString());

            await consumer.CloseAsync();
            consumer.Dispose();
        }
    }
}
