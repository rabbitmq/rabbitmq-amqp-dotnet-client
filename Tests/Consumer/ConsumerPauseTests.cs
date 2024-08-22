// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using EasyNetQ.Management.Client.Model;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Consumer;

public class ConsumerPauseTests(ITestOutputHelper testOutputHelper) : IDisposable
{
    private readonly HttpApiClient _httpApiClient = new();
    private readonly ITestOutputHelper _testOutputHelper = testOutputHelper;

    public void Dispose() => _httpApiClient.Dispose();

    [Fact]
    public async Task PauseShouldStopMessageArrivalUnpauseShouldResumeIt()
    {
        const int messageCount = 100;

        ConnectionSettingBuilder connectionSettingsBuilder = ConnectionSettingBuilder.Create();
        IConnectionSettings connectionSettings = connectionSettingsBuilder
            .ContainerId(nameof(PauseShouldStopMessageArrivalUnpauseShouldResumeIt)).Build();
        IConnection connection = await AmqpConnection.CreateAsync(connectionSettings);
        IManagement management = connection.Management();
        try
        {
            IQueueInfo declaredQueueInfo = await management.Queue().Exclusive(true).DeclareAsync();
            IPublisher publisher = await connection.PublisherBuilder().Queue(declaredQueueInfo.Name()).BuildAsync();

            var publishTasks = new List<Task<RabbitMQ.AMQP.Client.PublishResult>>();
            for (int i = 0; i < messageCount; i++)
            {
                int idx = i;
                IMessage message = new AmqpMessage($"message_{i}");
                publishTasks.Add(publisher.PublishAsync(message));
            }

            await Task.WhenAll(publishTasks);

            foreach (Task<RabbitMQ.AMQP.Client.PublishResult> pt in publishTasks)
            {
                RabbitMQ.AMQP.Client.PublishResult pr = await pt;
                Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);
            }

            const int initialCredits = 10;

            var messageContexts = new ConcurrentBag<IContext>();

            IConsumer consumer = await connection.ConsumerBuilder()
                .Queue(declaredQueueInfo.Name())
                .InitialCredits(initialCredits)
                .MessageHandler((IContext ctx, IMessage msg) =>
                {
                    messageContexts.Add(ctx);
                    return Task.CompletedTask;
                }).BuildAsync();

            Task<bool> WaitForMessageContextCountAtLeast(int expectedCount)
            {
                return Task.FromResult(messageContexts.Count >= expectedCount);
            }

            await SystemUtils.WaitUntilAsync(() => WaitForMessageContextCountAtLeast(initialCredits));

            IQueueInfo queueInfo = await management.GetQueueInfoAsync(declaredQueueInfo.Name());
            ulong expectedMessageCount = messageCount - initialCredits;
            Assert.Equal(expectedMessageCount, queueInfo.MessageCount());

            Queue? apiQueue = null;
            async Task<bool> MessagesUnacknowledgedIsEqualTo(long expectedMessagesUnacknowledged)
            {
                apiQueue = await _httpApiClient.GetQueueAsync(declaredQueueInfo.Name());
                return apiQueue.MessagesUnacknowledged == expectedMessagesUnacknowledged;
            }

            await SystemUtils.WaitUntilAsync(() => MessagesUnacknowledgedIsEqualTo(initialCredits));

            Assert.NotNull(apiQueue);
            Assert.Equal(initialCredits, apiQueue.MessagesUnacknowledged);
            Assert.Equal(messageContexts.Count, consumer.UnsettledMessageCount);

            consumer.Pause();

            var acceptTasks = new List<Task>();
            foreach (IContext ctx in messageContexts)
            {
                acceptTasks.Add(ctx.AcceptAsync());
            }

            async Task<bool> MessagesUnacknowledgedIsZero()
            {
                Queue apiQueue = await _httpApiClient.GetQueueAsync(declaredQueueInfo.Name());
                return apiQueue.MessagesUnacknowledged == 0;
            }
            await SystemUtils.WaitUntilAsync(MessagesUnacknowledgedIsZero);

            await Task.WhenAll(acceptTasks);
            acceptTasks.Clear();

            Assert.Equal(initialCredits, messageContexts.Count);
            Assert.Equal((uint)0, consumer.UnsettledMessageCount);
            messageContexts.Clear();

            consumer.Unpause();

            await SystemUtils.WaitUntilAsync(() => WaitForMessageContextCountAtLeast(initialCredits));

            queueInfo = await management.GetQueueInfoAsync(declaredQueueInfo.Name());
            expectedMessageCount = messageCount - (initialCredits * 2);
            Assert.Equal(expectedMessageCount, queueInfo.MessageCount());

            await SystemUtils.WaitUntilAsync(() => MessagesUnacknowledgedIsEqualTo(initialCredits));

            Assert.NotNull(apiQueue);
            Assert.Equal(initialCredits, apiQueue.MessagesUnacknowledged);
            Assert.Equal(messageContexts.Count, consumer.UnsettledMessageCount);

            consumer.Pause();

            foreach (IContext ctx in messageContexts)
            {
                acceptTasks.Add(ctx.AcceptAsync());
            }

            await Task.WhenAll(acceptTasks);
            acceptTasks.Clear();
        }
        finally
        {
            await management.CloseAsync();
            await connection.CloseAsync();
        }
    }
}
