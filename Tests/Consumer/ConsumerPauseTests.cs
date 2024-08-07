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

        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        try
        {
            IQueueInfo declaredQueueInfo = await management.Queue().Exclusive(true).Declare();
            IPublisher publisher = await connection.PublisherBuilder().Queue(declaredQueueInfo.Name()).BuildAsync();

            var publishTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            var publishTasks = new List<Task>();
            for (int i = 0; i < messageCount; i++)
            {
                int idx = i;
                IMessage message = new AmqpMessage($"message_{i}");
                publishTasks.Add(publisher.Publish(message,
                    (message, descriptor) =>
                    {
                        Assert.Equal(OutcomeState.Accepted, descriptor.State);
                        if (idx == (messageCount - 1))
                        {
                            publishTcs.SetResult();
                        }
                    }));
            }

            await Task.WhenAll(publishTasks);
            await publishTcs.Task;

            const int initialCredits = 10;

            var messageContexts = new ConcurrentBag<IContext>();

            IConsumer consumer = await connection.ConsumerBuilder()
                .Queue(declaredQueueInfo.Name())
                .InitialCredits(initialCredits)
                .MessageHandler((IContext ctx, IMessage msg) =>
                {
                    messageContexts.Add(ctx);
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
            async Task<bool> MessagesUnacknowledgedIsGreaterThanZero()
            {
                apiQueue = await _httpApiClient.GetQueueAsync(declaredQueueInfo.Name());
                return apiQueue.MessagesUnacknowledged > 0;
            }

            await SystemUtils.WaitUntilAsync(MessagesUnacknowledgedIsGreaterThanZero);

            Assert.NotNull(apiQueue);
            Assert.Equal(initialCredits, apiQueue.MessagesUnacknowledged);

            consumer.Pause();

            foreach (IContext ctx in messageContexts)
            {
                ctx.Accept();
            }

            async Task<bool> MessagesUnacknowledgedIsZero()
            {
                Queue apiQueue = await _httpApiClient.GetQueueAsync(declaredQueueInfo.Name());
                return apiQueue.MessagesUnacknowledged == 0;
            }

            await SystemUtils.WaitUntilAsync(MessagesUnacknowledgedIsZero);

            Assert.Equal(initialCredits, messageContexts.Count);

            consumer.Unpause();

            await SystemUtils.WaitUntilAsync(() => WaitForMessageContextCountAtLeast(initialCredits * 2));

            consumer.Pause();

            foreach (IContext ctx in messageContexts)
            {
                ctx.Accept();
            }
        }
        finally
        {
            await management.CloseAsync();
            await connection.CloseAsync();
        }
    }
}
