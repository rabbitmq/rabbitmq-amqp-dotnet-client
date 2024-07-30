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

public class AmqpTests : IntegrationTest
{
    private readonly string _queueName;
    private IManagement? _management;

    public AmqpTests(ITestOutputHelper testOutputHelper) : base(testOutputHelper)
    {
        _queueName = $"{_testDisplayName}-queue-{Now}";
    }

    public override async Task InitializeAsync()
    {
        await base.InitializeAsync();

        if (_connection is null)
        {
            throw new InvalidOperationException("_connection is null");
        }
        else
        {
            _management = _connection.Management();
        }
    }

    public override Task DisposeAsync()
    {
        if (_management is not null)
        {
            IQueueDeletion queueDeletion = _management.QueueDeletion();
            queueDeletion.Delete(_queueName);
            _management.Dispose();
        }

        return base.DisposeAsync();
    }

    [Fact]
    public async Task QueueInfoTest()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueInfo declaredQueueInfo = await _management.Queue(_queueName).Quorum().Queue().Declare();
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
        IQueueInfo declaredQueueInfo = await _management.Queue().Name(_queueName).Classic().Queue().Declare();
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
        string e1 = $"{prefix}-e1-{_testDisplayName}-{now}";
        string e2 = $"{prefix}-e2-{_testDisplayName}-{now}";
        string rk = $"{prefix}-foo-{now}";

        Dictionary<string, object> bindingArguments = new();
        if (addBindingArgments)
        {
            bindingArguments.Add("foo", prefix + "-bar");
        }

        await _management.Exchange().Name(e1).Type(ExchangeType.DIRECT).Declare();
        await _management.Exchange().Name(e2).Type(ExchangeType.FANOUT).Declare();
        await _management.Queue().Name(_queueName).Type(QueueType.CLASSIC).Declare();

        IBindingSpecification e1e2Binding = _management.Binding().SourceExchange(e1).DestinationExchange(e2).Key(rk).Arguments(bindingArguments);
        IBindingSpecification e2qBinding = _management.Binding().SourceExchange(e2).DestinationQueue(_queueName).Arguments(bindingArguments);

        await e1e2Binding.Bind();
        await e2qBinding.Bind();

        IPublisherBuilder publisherBuilder1 = _connection.PublisherBuilder();
        IPublisherBuilder publisherBuilder2 = _connection.PublisherBuilder();

        IPublisher publisher1 = await publisherBuilder1.Exchange(e1).Key(rk).BuildAsync();
        IPublisher publisher2 = await publisherBuilder2.Exchange(e2).BuildAsync();

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

        // TODO these fail with 400
        // await e1e2Binding.Unbind();
        // await e2qBinding.Unbind();

        await _management.ExchangeDeletion().Delete(e2);
        await _management.ExchangeDeletion().Delete(e1);
        // Note: DisposeAsync will delete the queue
    }
}
