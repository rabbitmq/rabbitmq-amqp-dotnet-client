// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests;

public class AmqpTests
{
    private readonly ITestOutputHelper _testOutputHelper;
    private readonly string _testDisplayName = nameof(AmqpTests);

    public AmqpTests(ITestOutputHelper testOutputHelper)
    {
        _testOutputHelper = testOutputHelper;

        Type type = _testOutputHelper.GetType();
        FieldInfo? testMember = type.GetField("test", BindingFlags.Instance | BindingFlags.NonPublic);
        if (testMember is not null)
        {
            object? testObj = testMember.GetValue(_testOutputHelper);
            if (testObj is ITest test)
            {
                _testDisplayName = test.DisplayName;
            }
        }
    }

    [Fact]
    public async Task QueueInfoTest()
    {
        string queueName = _testDisplayName;

        using IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());

        IManagement management = connection.Management();

        IQueueInfo declaredQueueInfo = await management.Queue(queueName).Quorum().Queue().Declare();
        IQueueInfo retrievedQueueInfo = await management.GetQueueInfoAsync(queueName);

        Assert.Equal(queueName, declaredQueueInfo.Name());
        Assert.Equal(queueName, retrievedQueueInfo.Name());

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

        await connection.CloseAsync();
    }
}
