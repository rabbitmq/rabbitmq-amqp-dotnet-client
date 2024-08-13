// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class AmqpTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    [Fact]
    public async Task QueueInfoTest()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueInfo declaredQueueInfo = await _management.Queue(_queueName).Quorum().Queue().DeclareAsync();
        IQueueInfo retrievedQueueInfo = await _management.GetQueueInfoAsync(_queueName);

        Assert.Equal(_queueName, declaredQueueInfo.Name());
        Assert.Equal(_queueName, retrievedQueueInfo.Name());

        Assert.Equal(QueueType.QUORUM, declaredQueueInfo.Type());
        Assert.Equal(QueueType.QUORUM, retrievedQueueInfo.Type());

        Assert.True(declaredQueueInfo.Durable());
        Assert.True(retrievedQueueInfo.Durable());

        Assert.False(declaredQueueInfo.AutoDelete());
        Assert.False(retrievedQueueInfo.AutoDelete());

        Assert.False(declaredQueueInfo.Exclusive());
        Assert.False(retrievedQueueInfo.Exclusive());

        Assert.Equal((ulong)0, declaredQueueInfo.MessageCount());
        Assert.Equal((ulong)0, retrievedQueueInfo.MessageCount());

        Assert.Equal((ulong)0, declaredQueueInfo.ConsumerCount());
        Assert.Equal((ulong)0, retrievedQueueInfo.ConsumerCount());

        Dictionary<string, object> declaredArgs = declaredQueueInfo.Arguments();
        Dictionary<string, object> retrievedArgs = retrievedQueueInfo.Arguments();
        Assert.True(declaredArgs.ContainsKey("x-queue-type"));
        Assert.True(retrievedArgs.ContainsKey("x-queue-type"));
        Assert.Equal(declaredArgs["x-queue-type"], "quorum");
        Assert.Equal(retrievedArgs["x-queue-type"], "quorum");
    }

    [Theory]
    [InlineData("foobar")]
    [InlineData("фообар")]
    public async Task QueueDeclareDeletePublishConsume(string subject)
    {
        byte[] messageBody = Encoding.UTF8.GetBytes("hello");
        const int messageCount = 100;

        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        // IQueueInfo declaredQueueInfo = await _management.Queue().Name(_queueName).Quorum().Queue().Declare();
        IQueueInfo declaredQueueInfo = await _management.Queue().Name(_queueName).Classic().Queue().DeclareAsync();
        Assert.Equal(_queueName, declaredQueueInfo.Name());

        IPublisherBuilder publisherBuilder = _connection.PublisherBuilder();
        IPublisher publisher = await publisherBuilder.Queue(declaredQueueInfo.Name()).BuildAsync();

        var publishTasks = new List<Task<PublishResult>>();
        for (int i = 0; i < messageCount; i++)
        {
            Guid messageId = Guid.NewGuid();

            IMessage message = new AmqpMessage(messageBody);
            message.MessageId(messageId.ToString());
            message.Subject(subject);
            publishTasks.Add(publisher.PublishAsync(message));
        }

        await WhenAllComplete(publishTasks);

        foreach (Task<PublishResult> pt in publishTasks)
        {
            PublishResult pr = await pt;
            Assert.Equal(OutcomeState.Accepted, pr.Outcome.State);
        }

        IQueueInfo retrievedQueueInfo0 = await _management.GetQueueInfoAsync(_queueName);
        Assert.Equal(_queueName, retrievedQueueInfo0.Name());
        Assert.Equal((uint)0, retrievedQueueInfo0.ConsumerCount());
        Assert.Equal((ulong)messageCount, retrievedQueueInfo0.MessageCount());

        long receivedMessageCount = 0;
        var allMessagesReceivedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        string? receivedSubject = null;
        async Task MessageHandler(IContext ctx, IMessage msg)
        {
            receivedSubject = msg.Subject();
            await ctx.AcceptAsync();
            if (Interlocked.Increment(ref receivedMessageCount) == messageCount)
            {
                allMessagesReceivedTcs.SetResult();
            }
        }

        IConsumerBuilder consumerBuilder = _connection.ConsumerBuilder();
        IConsumer consumer = await consumerBuilder.Queue(_queueName).MessageHandler(MessageHandler).BuildAsync();

        await WhenTaskCompletes(allMessagesReceivedTcs.Task);

        Assert.NotNull(receivedSubject);
        Assert.Equal(subject, receivedSubject);

        IQueueInfo retrievedQueueInfo1 = await _management.GetQueueInfoAsync(_queueName);
        Assert.Equal((uint)1, retrievedQueueInfo1.ConsumerCount());
        Assert.Equal((uint)0, retrievedQueueInfo1.MessageCount());

        await publisher.CloseAsync();
        await consumer.CloseAsync();
    }

    [Theory]
    [InlineData("foo", false)]
    [InlineData("foo", true)]
    [InlineData("фообар", true)]
    [InlineData("фообар", false)]
    [InlineData("фоо!бар", false)]
    [InlineData("фоо!бар", true)]
    public async Task BindingTest(string prefix, bool addBindingArgments)
    {
        byte[] messageBody = Encoding.UTF8.GetBytes("hello");

        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        string now = Now;
        string rkStr = $"{prefix}-foo-{now}";

        Dictionary<string, object> bindingArguments = new();
        if (addBindingArgments)
        {
            bindingArguments.Add("foo", prefix + "-bar");
        }

        IExchangeSpecification ex1spec = _management.Exchange($"{prefix}-e1-{_testDisplayName}-{now}").Type(ExchangeType.DIRECT);
        IExchangeSpecification ex2spec = _management.Exchange($"{prefix}-e2-{_testDisplayName}-{now}").Type(ExchangeType.FANOUT);
        IQueueSpecification queueSpec = _management.Queue(_queueName).Type(QueueType.CLASSIC);

        await ex1spec.DeclareAsync();
        await ex2spec.DeclareAsync();
        await queueSpec.DeclareAsync();

        IBindingSpecification e1e2Binding = _management.Binding()
            .SourceExchange(ex1spec)
            .DestinationExchange(ex2spec)
            .Key(rkStr)
            .Arguments(bindingArguments);
        IBindingSpecification e2qBinding = _management.Binding()
            .SourceExchange(ex2spec)
            .DestinationQueue(queueSpec)
            .Arguments(bindingArguments);

        await e1e2Binding.BindAsync();
        await e2qBinding.BindAsync();

        IPublisherBuilder publisherBuilder1 = _connection.PublisherBuilder();
        IPublisherBuilder publisherBuilder2 = _connection.PublisherBuilder();

        IPublisher publisher1 = await publisherBuilder1.Exchange(ex1spec).Key(rkStr).BuildAsync();
        IPublisher publisher2 = await publisherBuilder2.Exchange(ex2spec).BuildAsync();

        IMessage message = new AmqpMessage(messageBody);

        Task<PublishResult> publish1Task = publisher1.PublishAsync(message);
        Task<PublishResult> publish2Task = publisher2.PublishAsync(message);
        await WhenAllComplete([publish1Task, publish2Task]);

        PublishResult publish1Result = await publish1Task;
        Assert.Equal(OutcomeState.Accepted, publish1Result.Outcome.State);

        PublishResult publish2Result = await publish2Task;
        Assert.Equal(OutcomeState.Accepted, publish2Result.Outcome.State);

        const int expectedMessageCount = 2;
        long receivedMessageCount = 0;
        var allMessagesReceivedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        async Task MessageHandler(IContext ctx, IMessage msg)
        {
            await ctx.AcceptAsync();
            if (Interlocked.Increment(ref receivedMessageCount) == expectedMessageCount)
            {
                allMessagesReceivedTcs.SetResult();
            }
        }

        IConsumerBuilder consumerBuilder = _connection.ConsumerBuilder();
        IConsumer consumer = await consumerBuilder.Queue(_queueName).MessageHandler(MessageHandler).BuildAsync();

        await WhenTaskCompletes(allMessagesReceivedTcs.Task);

        await publisher1.CloseAsync();
        await publisher2.CloseAsync();
        await consumer.CloseAsync();

        await e1e2Binding.UnbindAsync();
        await e2qBinding.UnbindAsync();

        await ex2spec.DeleteAsync();
        await ex2spec.DeleteAsync();
        // Note: DisposeAsync will delete the queue
    }
}
