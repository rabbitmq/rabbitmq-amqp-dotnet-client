// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading;
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

        [Fact]
        public async Task TwoConsumersWithSingleActiveConsumer_OnlyFirstConsumerReceivesMessages()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            IQueueSpecification queueSpec = _management.Queue().Name(_queueName).SingleActiveConsumer(true);
            await queueSpec.DeclareAsync();

            const int messageCount = 3;
            int activeConsumerReceived = 0;
            int inactiveConsumerReceived = 0;
            var activeConsumerDone = CreateTaskCompletionSource<bool>();

            // First consumer: becomes the active one and should receive all messages
            IConsumer activeConsumer = await _connection.ConsumerBuilder()
                .Queue(queueSpec)
                .MessageHandler((context, message) =>
                {
                    context.Accept();
                    int received = Interlocked.Increment(ref activeConsumerReceived);
                    if (received == messageCount)
                    {
                        activeConsumerDone.SetResult(true);
                    }

                    return Task.CompletedTask;
                })
                .BuildAndStartAsync();

            // Second consumer: must remain inactive; with SAC only one consumer is active at a time
            IConsumer inactiveConsumer = await _connection.ConsumerBuilder()
                .Queue(queueSpec)
                .MessageHandler((context, message) =>
                {
                    context.Accept();
                    Interlocked.Increment(ref inactiveConsumerReceived);
                    return Task.CompletedTask;
                })
                .BuildAndStartAsync();

            await PublishAsync(queueSpec, (ulong)messageCount);

            await WhenTcsCompletes(activeConsumerDone);
            Assert.Equal(messageCount, activeConsumerReceived);
            Assert.Equal(0, inactiveConsumerReceived);

            await activeConsumer.CloseAsync();
            activeConsumer.Dispose();
            await inactiveConsumer.CloseAsync();
            inactiveConsumer.Dispose();
        }

        [SkippableFact]
        public async Task QuorumSingleActiveConsumerFlowState_StandbyPromotedWhenActiveCloses()
        {
            Skip.IfNot(_featureFlags is { IsQuorumSingleActiveConsumerFlowStateEnabled: true },
                "RabbitMQ 4.3+ required for quorum SAC FLOW link-state (rabbitmq:active).");

            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            IQueueSpecification queueSpec = _management.Queue()
                .Name(_queueName)
                .Quorum()
                .Queue()
                .SingleActiveConsumer(true);
            await queueSpec.DeclareAsync();

            var firstConsumerStates = new List<bool>();
            TaskCompletionSource<bool> secondPromoted = CreateTaskCompletionSource<bool>();

            IConsumer first = await _connection.ConsumerBuilder()
                .Queue(queueSpec).Quorum()
                .SingleActiveConsumerStateChanged((_, isActive) =>
                {
                    lock (firstConsumerStates)
                    {
                        firstConsumerStates.Add(isActive);
                    }
                }).Builder()
                .MessageHandler((ctx, _) =>
                {
                    ctx.Accept();
                    return Task.CompletedTask;
                })
                .BuildAndStartAsync();

            IConsumer second = await _connection.ConsumerBuilder()
                .Queue(queueSpec).Quorum()
                .SingleActiveConsumerStateChanged((_, isActive) =>
                {
                    if (isActive)
                    {
                        secondPromoted.TrySetResult(true);
                    }
                }).Builder()
                .MessageHandler((ctx, _) =>
                {
                    ctx.Accept();
                    return Task.CompletedTask;
                })
                .BuildAndStartAsync();

            await Task.Delay(500);

            Assert.Contains(true, firstConsumerStates);

            await first.CloseAsync();
            first.Dispose();

            await WhenTcsCompletes(secondPromoted);

            await second.CloseAsync();
            second.Dispose();
        }

        [Fact]
        public async Task SingleActiveConsumerStateChanged_ThrowsWithDirectReplyTo()
        {
            Assert.NotNull(_connection);
            Assert.NotNull(_management);

            IQueueSpecification queueSpec = _management.Queue().Name(_queueName).Quorum().Queue();
            await queueSpec.DeclareAsync();

            NotSupportedException ex = await Assert.ThrowsAsync<NotSupportedException>(async () =>
                await _connection.ConsumerBuilder()
                    .Queue(queueSpec).Quorum()
                    .SingleActiveConsumerStateChanged((_, _) => { }).Builder()
                    .SettleStrategy(ConsumerSettleStrategy.DirectReplyTo)
                    .MessageHandler((_, _) => Task.CompletedTask)
                    .BuildAndStartAsync());

            Assert.Contains("Single Active Consumer state change notification is not supported", ex.Message,
                StringComparison.OrdinalIgnoreCase);
        }
    }
}
