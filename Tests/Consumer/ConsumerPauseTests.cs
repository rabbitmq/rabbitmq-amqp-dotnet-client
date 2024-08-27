// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using EasyNetQ.Management.Client.Model;
using RabbitMQ.AMQP.Client;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Consumer;

public class ConsumerPauseTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper), IDisposable
{
    private readonly HttpApiClient _httpApiClient = new();

    public void Dispose() => _httpApiClient.Dispose();

    [Fact]
    public async Task PauseShouldStopMessageArrivalUnpauseShouldResumeIt()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        const int messageCount = 100;

        IQueueSpecification queueSpecification = _management.Queue().Exclusive(true);
        IQueueInfo declaredQueueInfo = await queueSpecification.DeclareAsync();

        await PublishAsync(queueSpecification, messageCount);

        const int initialCredits = 10;

        var messageContexts = new ConcurrentBag<IContext>();

        IConsumer consumer = await _connection.ConsumerBuilder()
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

        IQueueInfo queueInfo = await _management.GetQueueInfoAsync(declaredQueueInfo.Name());
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

        await WhenAllComplete(acceptTasks);
        acceptTasks.Clear();

        Assert.Equal(initialCredits, messageContexts.Count);
        Assert.Equal((uint)0, consumer.UnsettledMessageCount);
        messageContexts.Clear();

        consumer.Unpause();

        await SystemUtils.WaitUntilAsync(() => WaitForMessageContextCountAtLeast(initialCredits));

        queueInfo = await _management.GetQueueInfoAsync(declaredQueueInfo.Name());
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

        await WhenAllComplete(acceptTasks);
        acceptTasks.Clear();
    }

    [Fact]
    public async Task ConsumerPauseThenClose()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        const int messageCount = 100;

        IQueueSpecification queueSpecification = _management.Queue(_queueName).Exclusive(true);
        IQueueInfo queueInfo = await queueSpecification.DeclareAsync();

        await PublishAsync(queueSpecification, messageCount);

        const int initialCredits = 10;
        long receivedCount = 0;
        var unsettledMessages = new ConcurrentBag<IContext>();
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueInfo.Name())
            .InitialCredits(initialCredits)
            .MessageHandler(async (IContext ctx, IMessage msg) =>
            {
                if (Interlocked.Increment(ref receivedCount) < initialCredits)
                {
                    await ctx.AcceptAsync();
                }
                else
                {
                    unsettledMessages.Add(ctx);
                }
            }).BuildAsync();

        DateTime start = DateTime.Now;
        for (int i = 0; i < 100; i++)
        {
            int unsettledMessageCount0 = unsettledMessages.Count;
            await Task.Delay(TimeSpan.FromMilliseconds(200));
            int unsettledMessageCount1 = unsettledMessages.Count;
            if (unsettledMessageCount0 == unsettledMessageCount1)
            {
                break;
            }

            DateTime now = DateTime.Now;

            if (now - start > TimeSpan.FromSeconds(10))
            {
                Assert.Fail("unsettledMessageCount did not stabilize in 10 seconds");
            }
        }

        Assert.False(unsettledMessages.IsEmpty);

        consumer.Pause();

        long receivedCountAfterPause = Interlocked.Read(ref receivedCount);

        var acceptTasks = new List<Task>();
        foreach (IContext cxt in unsettledMessages)
        {
            acceptTasks.Add(cxt.AcceptAsync());
        }

        await WhenAllComplete(acceptTasks);

        await consumer.CloseAsync();
        consumer.Dispose();

        Assert.Equal(receivedCountAfterPause, receivedCount);

        IQueueInfo queueInfo1 = await _management.GetQueueInfoAsync(_queueName);
        ulong expectedMessageCount = (ulong)(messageCount - receivedCount);
        Assert.Equal(expectedMessageCount, queueInfo1.MessageCount());
    }

    [Fact]
    public async Task ConsumerGracefulShutdownExample()
    {
        var r = new Random();
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        const int messageCount = 100;

        IQueueSpecification queueSpecification = _management.Queue(_queueName).Exclusive(true);
        IQueueInfo queueInfo = await queueSpecification.DeclareAsync();

        await PublishAsync(queueSpecification, messageCount);

        TaskCompletionSource<bool> receivedTwiceInitialCreditsTcs = CreateTaskCompletionSource();
        const int initialCredits = 10;
        long receivedCount = 0;
        var unsettledMessages = new ConcurrentBag<IContext>();
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(queueInfo.Name())
            .InitialCredits(initialCredits)
            .MessageHandler(async (IContext ctx, IMessage msg) =>
            {
                if (Interlocked.Increment(ref receivedCount) > (initialCredits * 2))
                {
                    receivedTwiceInitialCreditsTcs.TrySetResult(true);
                }
                await Task.Delay(TimeSpan.FromMilliseconds(r.Next(1, 10)));
                await ctx.AcceptAsync();
            }).BuildAsync();

        await WhenTcsCompletes(receivedTwiceInitialCreditsTcs);

        consumer.Pause();

        DateTime start = DateTime.Now;
        while (consumer.UnsettledMessageCount != 0)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(100));
            DateTime now = DateTime.Now;
            if (start - now > _waitSpan)
            {
                Assert.Fail("consumer.UnsettledMessageCount never reached zero!");
            }
        }

        await consumer.CloseAsync();
        consumer.Dispose();

        IQueueInfo queueInfo1 = await _management.GetQueueInfoAsync(queueSpecification);
        ulong expectedMessageCount = (ulong)(messageCount - receivedCount);
        Assert.Equal(expectedMessageCount, queueInfo1.MessageCount());
    }
}
