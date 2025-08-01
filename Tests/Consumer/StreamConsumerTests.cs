// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amqp.Types;
using RabbitMQ.AMQP.Client;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Consumer;

/// <summary>
/// These tests are only for the streaming part of the consumer.
/// we'd need to test the consumer itself in a different use cases
/// like restarting from an offset with and without the subscription listener
/// </summary>
public class StreamConsumerTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    [Fact]
    public async Task StreamConsumerOptionsOffsetLong()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        const int messageCount = 100;

        IQueueSpecification q = _management.Queue(_queueName).Stream().Queue();
        await q.DeclareAsync();

        await PublishAsync(q, messageCount);

        SemaphoreSlim offsetsSemaphore = new(1, 1);
        List<long> offsets = new(messageCount);

        TaskCompletionSource<bool> tcs0 = CreateTaskCompletionSource();
        TaskCompletionSource<bool> tcs1 = CreateTaskCompletionSource();
        ConcurrentQueue<TaskCompletionSource<bool>> tcsQue = new();
        tcsQue.Enqueue(tcs0);
        tcsQue.Enqueue(tcs1);

        long expectedMessageCount = messageCount;
        long receivedCount = 0;
        async Task MessageHandler(IContext cxt, IMessage msg)
        {
            await offsetsSemaphore.WaitAsync();
            try
            {
                cxt.Accept();

                offsets.Add((long)msg.Annotation("x-stream-offset"));

                receivedCount++;
                if (receivedCount == expectedMessageCount)
                {
                    if (tcsQue.TryDequeue(out TaskCompletionSource<bool>? tcs))
                    {
                        tcs.SetResult(true);
                    }
                }
            }
            catch (Exception ex)
            {
                if (tcsQue.TryDequeue(out TaskCompletionSource<bool>? tcs))
                {
                    tcs.SetException(ex);
                }
            }
            finally
            {
                offsetsSemaphore.Release();
            }
        }

        IConsumerBuilder consumerBuilder0 = _connection.ConsumerBuilder()
            .Queue(_queueName)
            .Stream()
            .Offset((long)0)
            .Builder()
            .MessageHandler(MessageHandler);

        using (IConsumer consumer = await consumerBuilder0.BuildAndStartAsync())
        {
            await WhenTcsCompletes(tcs0);
            await consumer.CloseAsync();
        }

        receivedCount = 0;

        offsets.Sort();
        int halfwayOffset = (int)(offsets.Last() / 2);
        List<long> offsetTail = new();
        for (int i = halfwayOffset; i < offsets.Count; i++)
        {
            offsetTail.Add(offsets[i]);
        }

        expectedMessageCount = offsetTail.Count;

        IConsumerBuilder consumerBuilder1 = _connection.ConsumerBuilder()
            .Queue(_queueName)
            .Stream()
            .Offset(offsetTail.First())
            .Builder()
            .MessageHandler(MessageHandler);

        using (IConsumer consumer = await consumerBuilder0.BuildAndStartAsync())
        {
            await WhenTcsCompletes(tcs1);
            await consumer.CloseAsync();
        }
    }

    [Theory]
    [InlineData(StreamOffsetSpecification.First)]
    [InlineData(StreamOffsetSpecification.Next)]
    [InlineData(StreamOffsetSpecification.Last)]
    public async Task StreamConsumerOptionsOffset(StreamOffsetSpecification streamOffsetSpecification)
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        const int messageCount = 100;

        IQueueSpecification q = _management.Queue(_queueName).Stream().Queue();
        await q.DeclareAsync();

        await PublishAsync(q, messageCount);

        TaskCompletionSource<bool> tcs = CreateTaskCompletionSource();
        long firstOffset = -1;
        long receivedCount = 0;
        Task MessageHandler(IContext cxt, IMessage msg)
        {
            try
            {
                cxt.Accept();

                Interlocked.CompareExchange(ref firstOffset, (long)msg.Annotation("x-stream-offset"), -1);

                if (Interlocked.Increment(ref receivedCount) == messageCount)
                {
                    tcs.SetResult(true);
                }
            }
            catch (Exception ex)
            {
                tcs.SetException(ex);
            }

            return Task.CompletedTask;
        }

        IConsumerBuilder consumerBuilder0 = _connection.ConsumerBuilder()
            .Queue(_queueName)
            .Stream()
            .Offset(streamOffsetSpecification)
            .Builder()
            .MessageHandler(MessageHandler);

        IConsumer consumer = await consumerBuilder0.BuildAndStartAsync();
        try
        {
            Task publishTask = PublishAsync(q, messageCount);

            await WhenTcsCompletes(tcs);
            await WhenTaskCompletes(publishTask);

            if (streamOffsetSpecification == StreamOffsetSpecification.First)
            {
                Assert.Equal(0, firstOffset);
            }
            else
            {
                Assert.True(firstOffset > 0);
            }
        }
        finally
        {
            await consumer.CloseAsync();
            consumer.Dispose();
        }
    }

    [Theory]
    [InlineData("10m")]
    [InlineData(10)]
    public async Task StreamConsumerOptionsOffsetInterval(object interval)
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        const int messageCount = 100;

        IQueueSpecification q = _management.Queue(_queueName).Stream().Queue();
        await q.DeclareAsync();

        await PublishAsync(q, messageCount);

        TaskCompletionSource<bool> tcs = CreateTaskCompletionSource();
        long receivedCount = 0;
        Task MessageHandler(IContext cxt, IMessage msg)
        {
            try
            {
                cxt.Accept();

                if (Interlocked.Increment(ref receivedCount) == messageCount)
                {
                    tcs.SetResult(true);
                }
            }
            catch (Exception ex)
            {
                tcs.SetException(ex);
            }

            return Task.CompletedTask;
        }

        IConsumerBuilder.IStreamOptions streamOptions = _connection.ConsumerBuilder()
            .Queue(q)
            .Stream();
        if (interval is string intervalStr)
        {
            streamOptions.Offset(intervalStr);
        }
        else if (interval is int timeSpanMinutes)
        {
            DateTime startTime = DateTime.Now - TimeSpan.FromMinutes(timeSpanMinutes);
            streamOptions.Offset(startTime);
        }
        else
        {
            Assert.Fail();
        }

        IConsumerBuilder consumerBuilder = streamOptions.Builder()
            .MessageHandler(MessageHandler);
        using (IConsumer consumer = await consumerBuilder.BuildAndStartAsync())
        {
            await WhenTcsCompletes(tcs);
            await consumer.CloseAsync();
        }
    }

    [Fact]
    public void StreamConsumerOptionsOffsetIntervalWithInvalidSyntaxShouldThrow()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        Assert.ThrowsAny<ArgumentOutOfRangeException>(() =>
        {
            _connection.ConsumerBuilder().Stream().Offset("foo");
        });
    }

    /// <summary>
    /// Given a stream, if the connection is killed the consumer will restart consuming from the beginning
    /// because of Offset(StreamOffsetSpecification.First)
    /// so the total number of consumed messages should be 10 two times = 20
    /// </summary>
    [Fact]
    public async Task StreamConsumerBuilderShouldRestartFromTheBeginning()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        ConcurrentQueue<TaskCompletionSource<bool>> tcsQue = new();
        TaskCompletionSource<bool> tcs0 = new();
        tcsQue.Enqueue(tcs0);
        TaskCompletionSource<bool> tcs1 = new();
        tcsQue.Enqueue(tcs1);

        IQueueSpecification q = _management.Queue(_queueName).Stream().Queue();
        await q.DeclareAsync();

        await PublishAsync(q, 10);

        int totalConsumed = 0;
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(_queueName).InitialCredits(10).MessageHandler(
                (context, message) =>
                {
                    try
                    {
                        Interlocked.Increment(ref totalConsumed);
                        context.Accept();
                        if ((string)message.MessageId() == "9")
                        {
                            if (tcsQue.TryDequeue(out TaskCompletionSource<bool>? tcs))
                            {
                                tcs.SetResult(true);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        if (tcsQue.TryDequeue(out TaskCompletionSource<bool>? tcs))
                        {
                            tcs.SetException(ex);
                        }
                    }
                    return Task.CompletedTask;
                }
            ).Stream().Offset(StreamOffsetSpecification.First).Builder().BuildAndStartAsync();

        await WhenTcsCompletes(tcs0);

        await WaitUntilConnectionIsKilled(_containerId);

        await WhenTcsCompletes(tcs1);

        Assert.Equal(20, totalConsumed);
        await consumer.CloseAsync();
    }

    /// <summary>
    /// This is a standard case for the stream consumer with SubscriptionListener
    /// The consumer should start from the offset 5 and consume 5 messages
    /// Since: ctx.StreamOptions.Offset(5)
    /// </summary>
    [Fact]
    public async Task StreamConsumerBuilderShouldStartFromTheListenerConfiguration()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        TaskCompletionSource<bool> tcs = new();

        IQueueSpecification q = _management.Queue(_queueName).Stream().Queue();
        await q.DeclareAsync();

        await PublishAsync(q, 10);

        int totalConsumed = 0;
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(_queueName).Stream().FilterMatchUnfiltered(true).Offset(StreamOffsetSpecification.First).Builder()
            .InitialCredits(10).MessageHandler(
                (context, message) =>
                {
                    try
                    {
                        Interlocked.Increment(ref totalConsumed);
                        context.Accept();
                        if ((string)message.MessageId() == "9")
                        {
                            tcs.SetResult(true);
                        }
                    }
                    catch (Exception ex)
                    {
                        tcs.SetException(ex);
                    }
                    return Task.CompletedTask;
                }
            ).Stream().Builder().SubscriptionListener(
                ctx => { ctx.StreamOptions.Offset(5); }
            ).BuildAndStartAsync();

        await WhenTcsCompletes(tcs);
        Assert.Equal(5, totalConsumed);
        await consumer.CloseAsync();
    }

    /// <summary>
    /// In this test we simulate a listener that changes the offset after the connection is killed
    /// We simulate this by changing the offset from 4 to 6 like loading the offset from an external storage
    /// Each time the consumer is created the listener will change the offset and must be called to set the new offset
    /// </summary>
    [Fact]
    public async Task StreamConsumerBuilderShouldStartFromTheListenerConfigurationWhenConnectionIsKilled()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        ConcurrentQueue<TaskCompletionSource<bool>> tcsQue = new();
        TaskCompletionSource<bool> tcs0 = CreateTaskCompletionSource();
        tcsQue.Enqueue(tcs0);
        TaskCompletionSource<bool> tcs1 = CreateTaskCompletionSource();
        tcsQue.Enqueue(tcs1);

        IQueueSpecification q = _management.Queue(_queueName).Stream().Queue();
        await q.DeclareAsync();

        await PublishAsync(q, 10);

        int totalConsumed = 0;
        int startFrom = 2;
        IConsumer consumer = await _connection.ConsumerBuilder()
            .Queue(_queueName).InitialCredits(10).MessageHandler(
                (context, message) =>
                {
                    try
                    {
                        Interlocked.Increment(ref totalConsumed);
                        context.Accept();
                        if ((string)message.MessageId() == "9")
                        {
                            if (tcsQue.TryDequeue(out TaskCompletionSource<bool>? tcs))
                            {
                                tcs.SetResult(true);
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        if (tcsQue.TryDequeue(out TaskCompletionSource<bool>? tcs))
                        {
                            tcs.SetException(ex);
                        }
                    }
                    return Task.CompletedTask;
                }
            ).Stream()
            .Offset(StreamOffsetSpecification.First) // in this case this value is ignored because of the listener will replace it
            .Builder().SubscriptionListener(
                ctx =>
                {
                    // Here we simulate a listener that changes the offset after the connection is killed
                    // Like loading the offset from an external storage
                    // so first start from 4 then start from 6
                    // Given 10 messages, we should consume 6 messages first time 
                    // then 4 messages the second time the total should be 10
                    startFrom += 2;
                    ctx.StreamOptions.Offset(startFrom);
                    // In this case we should not be able to call the builder
                    Assert.Throws<NotImplementedException>(() => ctx.StreamOptions.Builder());
                }
            ).BuildAndStartAsync();

        await WhenTcsCompletes(tcs0);
        Assert.Equal(6, totalConsumed);

        await WaitUntilConnectionIsKilled(_containerId);

        await WhenTcsCompletes(tcs1);
        Assert.Equal(10, totalConsumed);

        await consumer.CloseAsync();
    }

    /// <summary>
    /// Port of streamFiltering
    /// </summary>
    [Fact]
    public async Task StreamFiltering()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        const int messageWaveCount = 100;
        string[] waves = ["apple", "orange", string.Empty, "banana"];
        int waveCount = waves.Length;
        int allMessagesCount = waveCount * messageWaveCount;

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName).Type(QueueType.STREAM);
        await queueSpec.DeclareAsync();

        foreach (string w in waves)
        {
            void ml(ulong idx, IMessage msg)
            {
                msg.MessageId(idx)
                    .Annotation("x-stream-filter-value", w);
            }

            /*
             * Note:
             * If publishing is done async, then messages must be filtered
             * as they are received because you could get "wrong" values
             * due to how the bloom filter works
             */
            await PublishAsync(queueSpec, messageWaveCount, ml);
        }

        long receivedCount = 0;
        Exception? messageHandlerEx = null;
        Task MessageHandler(IContext cxt, IMessage msg)
        {
            try
            {
                Interlocked.Increment(ref receivedCount);
                // _testOutputHelper.WriteLine($"id {0} x-stream-filter-value {1}",
                //    msg.MessageId(), msg.Annotation("x-stream-filter-value"));
                cxt.Accept();
            }
            catch (Exception ex)
            {
                messageHandlerEx = ex;
            }
            return Task.CompletedTask;
        }

        IConsumerBuilder consumerBuilder0 = _connection.ConsumerBuilder()
            .Queue(_queueName)
            .Stream()
            .Offset(StreamOffsetSpecification.First)
            .FilterValues("banana")
            .FilterMatchUnfiltered(false)
            .Builder()
            .MessageHandler(MessageHandler);

        using (IConsumer consumer = await consumerBuilder0.BuildAndStartAsync())
        {
            await WaitUntilStable<long>(() => Interlocked.Read(ref receivedCount));
            await consumer.CloseAsync();
        }

        // _testOutputHelper.WriteLine("0: receivedCount: {0}", receivedCount);
        Assert.Null(messageHandlerEx);
        Assert.True(receivedCount >= messageWaveCount);
        Assert.True(receivedCount < allMessagesCount,
            $"0: receivedCount {receivedCount}, waveCount * messageWaveCount {allMessagesCount}");
        receivedCount = 0;

        IConsumerBuilder consumerBuilder1 = _connection.ConsumerBuilder()
            .Queue(_queueName)
            .Stream()
            .Offset(StreamOffsetSpecification.First)
            .FilterValues("banana")
            .FilterMatchUnfiltered(true) // NOTE: true
            .Builder()
            .MessageHandler(MessageHandler);

        using (IConsumer consumer = await consumerBuilder1.BuildAndStartAsync())
        {
            await WaitUntilStable(() => (int)Interlocked.Read(ref receivedCount));
            await consumer.CloseAsync();
        }

        // _testOutputHelper.WriteLine("1: receivedCount: {0}", receivedCount);
        Assert.Null(messageHandlerEx);
        Assert.True(receivedCount >= 2 * messageWaveCount);
        Assert.True(receivedCount < allMessagesCount,
            $"1: messages.Count {receivedCount}, waveCount * messageWaveCount {allMessagesCount}");
    }

    /// <summary>
    /// Port of filterExpressionApplicationProperties
    /// </summary>
    [SkippableFact]
    public async Task FilterExpressionApplicationProperties()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);
        SkipIfNoFilterExpressions();

        const int messageCount = 10;

        IQueueSpecification q = _management.Queue(_queueName).Stream().Queue();
        await q.DeclareAsync();

        bool expected0 = true;
        await PublishAsync(q, messageCount, (_, msg) => msg.Property("foo", expected0));

        int expected1 = 42;
        await PublishAsync(q, messageCount, (_, msg) => msg.Property("foo", expected1));

        double expected2 = 42.1;
        await PublishAsync(q, messageCount, (_, msg) => msg.Property("foo", expected2));

        DateTime expected3 = DateTime.Now;
        await PublishAsync(q, messageCount, (_, msg) => msg.Property("foo", expected3));

        Guid expected4 = Guid.NewGuid();
        await PublishAsync(q, messageCount, (_, msg) => msg.Property("foo", expected4));

        byte[] expected5 = RandomBytes(128);
        await PublishAsync(q, messageCount, (_, msg) => msg.Property("foo", expected5));

        const string expected6 = "bar";
        await PublishAsync(q, messageCount, (_, msg) => msg.Property("foo", expected6));

        const string expected7 = "baz";
        await PublishAsync(q, messageCount, (_, msg) => msg.Property("foo", expected7));

        await PublishAsync(q, messageCount, (_, msg) => msg.Property("foo", expected6));

        const string expected8 = "symbol";
        await PublishAsync(q, messageCount, (_, msg) => msg.PropertySymbol("foo", expected8));

        await PublishAsync(q, messageCount, (_, msg) => msg.Property("foo", expected6).Property("k1", expected1));

        IEnumerable<IMessage> msgs = await ConsumeAsync(messageCount, options => options.Property("foo", expected0));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected0, (bool)m.Property("foo"));
        }

        msgs = await ConsumeAsync(messageCount, options => options.Property("foo", expected1));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected1, (int)m.Property("foo"));
        }

        msgs = await ConsumeAsync(messageCount, options => options.Property("foo", expected2));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected2, (double)m.Property("foo"));
        }

        /*
         * Note:
         * In this case, accuracy is only to 3 decimal
         *   Expected: 2024-10-31T20:32:25.0981361Z
         *   Actual:   2024-10-31T20:32:25.0980000Z
         */
        DateTime expected3Utc = expected3.ToUniversalTime();
        msgs = await ConsumeAsync(messageCount, options => options.Property("foo", expected3));
        foreach (IMessage m in msgs)
        {
            DateTime dt = (DateTime)m.Property("foo");
            Assert.Equal(expected3Utc.Date, dt.Date);
            Assert.Equal(expected3Utc.Year, dt.Year);
            Assert.Equal(expected3Utc.Month, dt.Month);
            Assert.Equal(expected3Utc.Day, dt.Day);
            Assert.Equal(expected3Utc.Hour, dt.Hour);
            Assert.Equal(expected3Utc.Minute, dt.Minute);
            Assert.Equal(expected3Utc.Second, dt.Second);
            Assert.Equal(expected3Utc.DayOfWeek, dt.DayOfWeek);
            Assert.Equal(expected3Utc.DayOfYear, dt.DayOfYear);
        }

        msgs = await ConsumeAsync(messageCount, options => options.Property("foo", expected4));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected4, (Guid)m.Property("foo"));
        }

        msgs = await ConsumeAsync(messageCount, options => options.Property("foo", expected5));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected5, (byte[])m.Property("foo"));
        }

        msgs = await ConsumeAsync(messageCount * 3, options => options.Property("foo", expected6));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected6, (string)m.Property("foo"));
        }

        msgs = await ConsumeAsync(messageCount * 4, options => options.Property("foo", "&p:b"));
        foreach (IMessage m in msgs)
        {
            Assert.StartsWith("b", (string)m.Property("foo"));
        }

        Symbol expected8symbol = new(expected8);
        msgs = await ConsumeAsync(messageCount, options => options.PropertySymbol("foo", "symbol"));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected8symbol, m.Property("foo"));
        }

        msgs = await ConsumeAsync(messageCount, options => options.Property("foo", expected6).Property("k1", expected1));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected6, (string)m.Property("foo"));
            Assert.Equal(expected1, (int)m.Property("k1"));
        }
    }

    /// <summary>
    /// Port of filterExpressionProperties
    /// </summary>
    [SkippableFact]
    public async Task FilterExpressionProperties()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);
        SkipIfNoFilterExpressions();

        const int messageCount = 10;

        IQueueSpecification q = _management.Queue(_queueName).Stream().Queue();
        await q.DeclareAsync();

        const ulong expected0 = 42;
        await PublishAsync(q, messageCount, (_, msg) => msg.MessageId(expected0));
        await PublishAsync(q, messageCount, (_, msg) => msg.MessageId(expected0 * 2));

        Guid expected1 = Guid.NewGuid();
        await PublishAsync(q, messageCount, (_, msg) => msg.MessageId(expected1));

        const ulong expected2 = 43;
        await PublishAsync(q, messageCount, (_, msg) => msg.CorrelationId(expected2));
        await PublishAsync(q, messageCount, (_, msg) => msg.CorrelationId(expected2 * 2));

        Guid expected3 = Guid.NewGuid();
        await PublishAsync(q, messageCount, (_, msg) => msg.CorrelationId(expected3));

        const string userIdStr = "guest";
        byte[] expected4 = Encoding.UTF8.GetBytes(userIdStr);
        await PublishAsync(q, messageCount, (_, msg) => msg.UserId(expected4));

        const string expected5 = "to foo bar";
        await PublishAsync(q, messageCount, (_, msg) => msg.To(expected5));
        await PublishAsync(q, messageCount, (_, msg) => msg.To("to foo baz"));

        const string expected6 = "subject foo bar";
        await PublishAsync(q, messageCount, (_, msg) => msg.Subject(expected6));
        await PublishAsync(q, messageCount, (_, msg) => msg.Subject("subject foo baz"));

        const string expected7 = "reply-to foo bar";
        await PublishAsync(q, messageCount, (_, msg) => msg.ReplyTo(expected7));
        await PublishAsync(q, messageCount, (_, msg) => msg.ReplyTo("reply-to foo baz"));

        const string expected8 = "text/plain";
        await PublishAsync(q, messageCount, (_, msg) => msg.ContentType(expected8));
        await PublishAsync(q, messageCount, (_, msg) => msg.ContentType("text/html"));

        const string expected9 = "gzip";
        await PublishAsync(q, messageCount, (_, msg) => msg.ContentEncoding(expected9));
        await PublishAsync(q, messageCount, (_, msg) => msg.ContentEncoding("zstd"));

        DateTime expected10 = DateTime.Now;
        await PublishAsync(q, messageCount, (_, msg) => msg.AbsoluteExpiryTime(expected10));
        await PublishAsync(q, messageCount, (_, msg) => msg.AbsoluteExpiryTime(expected10 + TimeSpan.FromMinutes(10)));
        await PublishAsync(q, messageCount, (_, msg) => msg.CreationTime(expected10));
        await PublishAsync(q, messageCount, (_, msg) => msg.CreationTime(expected10 + TimeSpan.FromMinutes(10)));

        const string expected11 = "group-id foo bar";
        await PublishAsync(q, messageCount, (_, msg) => msg.GroupId(expected11));
        await PublishAsync(q, messageCount, (_, msg) => msg.GroupId("group-id foo baz"));

        const uint expected12 = 44;
        await PublishAsync(q, messageCount, (_, msg) => msg.GroupSequence(expected12));
        await PublishAsync(q, messageCount, (_, msg) => msg.GroupSequence(expected12 * 2));

        const string expected13 = "reply-to-group-id foo bar";
        await PublishAsync(q, messageCount, (_, msg) => msg.ReplyToGroupId(expected13));
        await PublishAsync(q, messageCount, (_, msg) => msg.ReplyToGroupId("reply-to-group-id foo baz"));

        IEnumerable<IMessage> msgs = [];

        foreach (object expected in new object[] { expected0, expected1 })
        {
            msgs = await ConsumeAsync(messageCount, options => options.MessageId(expected));
            foreach (IMessage m in msgs)
            {
                Assert.Equal(expected, m.MessageId());
            }
        }

        foreach (object expected in new object[] { expected2, expected3 })
        {
            msgs = await ConsumeAsync(messageCount, options => options.CorrelationId(expected));
            foreach (IMessage m in msgs)
            {
                Assert.Equal(expected, m.CorrelationId());
            }
        }

        msgs = await ConsumeAsync(messageCount, options => options.UserId(expected4));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected4, m.UserId());
        }

        msgs = await ConsumeAsync(messageCount, options => options.To(expected5));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected5, m.To());
        }

        msgs = await ConsumeAsync(messageCount, options => options.Subject(expected6));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected6, m.Subject());
        }

        msgs = await ConsumeAsync(messageCount, options => options.ReplyTo(expected7));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected7, m.ReplyTo());
        }

        msgs = await ConsumeAsync(messageCount, options => options.ContentType(expected8));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected8, m.ContentType());
        }

        msgs = await ConsumeAsync(messageCount, options => options.ContentEncoding(expected9));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected9, m.ContentEncoding());
        }

        /*
         * Note:
         * In this case, accuracy is only to 3 decimal
         */
        DateTime expected10Utc = expected10.ToUniversalTime();
        msgs = await ConsumeAsync(messageCount, options => options.AbsoluteExpiryTime(expected10));
        foreach (IMessage m in msgs)
        {
            DateTime dt = m.AbsoluteExpiryTime();
            Assert.Equal(expected10Utc.Date, dt.Date);
            Assert.Equal(expected10Utc.Year, dt.Year);
            Assert.Equal(expected10Utc.Month, dt.Month);
            Assert.Equal(expected10Utc.Day, dt.Day);
            Assert.Equal(expected10Utc.Hour, dt.Hour);
            Assert.Equal(expected10Utc.Minute, dt.Minute);
            Assert.Equal(expected10Utc.Second, dt.Second);
            Assert.Equal(expected10Utc.DayOfWeek, dt.DayOfWeek);
            Assert.Equal(expected10Utc.DayOfYear, dt.DayOfYear);
        }

        msgs = await ConsumeAsync(messageCount, options => options.CreationTime(expected10));
        foreach (IMessage m in msgs)
        {
            DateTime dt = m.CreationTime();
            Assert.Equal(expected10Utc.Date, dt.Date);
            Assert.Equal(expected10Utc.Year, dt.Year);
            Assert.Equal(expected10Utc.Month, dt.Month);
            Assert.Equal(expected10Utc.Day, dt.Day);
            Assert.Equal(expected10Utc.Hour, dt.Hour);
            Assert.Equal(expected10Utc.Minute, dt.Minute);
            Assert.Equal(expected10Utc.Second, dt.Second);
            Assert.Equal(expected10Utc.DayOfWeek, dt.DayOfWeek);
            Assert.Equal(expected10Utc.DayOfYear, dt.DayOfYear);
        }

        msgs = await ConsumeAsync(messageCount, options => options.GroupId(expected11));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected11, m.GroupId());
        }

        msgs = await ConsumeAsync(messageCount, options => options.GroupSequence(expected12));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected12, m.GroupSequence());
        }

        msgs = await ConsumeAsync(messageCount, options => options.ReplyToGroupId(expected13));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(expected13, m.ReplyToGroupId());
        }
    }

    /// <summary>
    /// Port of filterExpressionPropertiesAndApplicationProperties
    /// </summary>
    [SkippableFact]
    public async Task FilterExpressionPropertiesAndApplicationProperties()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);
        SkipIfNoFilterExpressions();

        const int messageCount = 10;
        string subject = RandomString();
        string appKey = RandomString();
        int appValue = RandomNext();
        byte[] body1 = RandomBytes();
        byte[] body2 = RandomBytes();
        byte[] body3 = RandomBytes();

        IQueueSpecification q = _management.Queue(_queueName).Stream().Queue();
        await q.DeclareAsync();

        await PublishAsync(q, messageCount, (_, msg) => msg.Subject(subject).Body(body1));
        await PublishAsync(q, messageCount, (_, msg) => msg.Property(appKey, appValue).Body(body2));
        await PublishAsync(q, messageCount, (_, msg) => msg.Subject(subject).Property(appKey, appValue).Body(body3));

        IEnumerable<IMessage> msgs = await ConsumeAsync(messageCount * 2, options => options.Subject(subject));
        foreach (IMessage? m in msgs.Take(messageCount))
        {
            object mb = m.Body();
            Assert.Equal(body1, mb);
        }

        foreach (IMessage? m in msgs.Skip(messageCount).Take(messageCount))
        {
            Assert.Equal(body3, m.Body());
        }

        msgs = await ConsumeAsync(messageCount * 2, options => options.Property(appKey, appValue));
        foreach (IMessage? m in msgs.Take(messageCount))
        {
            object mb = m.Body();
            Assert.Equal(body2, mb);
        }

        foreach (IMessage? m in msgs.Skip(messageCount).Take(messageCount))
        {
            Assert.Equal(body3, m.Body());
        }

        msgs = await ConsumeAsync(messageCount, options => options.Subject(subject).Property(appKey, appValue));
        foreach (IMessage? m in msgs.Take(messageCount))
        {
            object mb = m.Body();
            Assert.Equal(body3, mb);
        }
    }

    /// <summary>
    /// Port of filterExpressionFilterFewMessagesFromManyToTestFlowControl
    /// </summary>
    [SkippableFact]
    public async Task FilterExpressionFilterFewMessagesFromManyToTestFlowControl()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);
        SkipIfNoFilterExpressions();

        string groupId = RandomString();

        IQueueSpecification q = _management.Queue(_queueName).Stream().Queue();
        await q.DeclareAsync();

        await PublishAsync(q, 1, (_, msg) => msg.GroupId(groupId));
        await PublishAsync(q, 1000);
        await PublishAsync(q, 1, (_, msg) => msg.GroupId(groupId));

        IEnumerable<IMessage> msgs = await ConsumeAsync(2, options => options.GroupId(groupId));
        foreach (IMessage m in msgs)
        {
            Assert.Equal(groupId, m.GroupId());
        }
    }

    /// <summary>
    /// Port of filterExpressionStringModifier
    /// </summary>
    [SkippableFact]
    public async Task FilterExpressionStringModifier()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);
        SkipIfNoFilterExpressions();

        IQueueSpecification q = _management.Queue(_queueName).Stream().Queue();
        await q.DeclareAsync();

        await PublishAsync(q, 1, (_, msg) => msg.Subject("abc 123"));
        await PublishAsync(q, 1, (_, msg) => msg.Subject("foo bar"));
        await PublishAsync(q, 1, (_, msg) => msg.Subject("ab 12"));

        IEnumerable<IMessage> msgs = await ConsumeAsync(2, options => options.Subject("&p:ab"));
        foreach (IMessage m in msgs)
        {
            Assert.StartsWith("ab", m.Subject());
        }

        msgs = await ConsumeAsync(1, options => options.Subject("&s:bar"));
        foreach (IMessage m in msgs)
        {
            Assert.Equal("foo bar", m.Subject());
        }
    }

    private void SkipIfNoFilterExpressions()
    {
        Skip.IfNot(_featureFlags is { IsFilterFeatureEnabled: true }, "At least RabbitMQ 4.1.0 required");
    }
}
