// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;

namespace Tests.Management;

public class ManagementTests(ITestOutputHelper testOutputHelper) : IntegrationTest(testOutputHelper)
{
    /// <summary>
    /// Test to validate the queue declaration with the auto generated name.
    /// The auto generated name is a client side generated.
    /// The test validates all the queue types.  
    /// </summary>
    /// <param name="type"> queues type</param>
    [Theory]
    [InlineData(QueueType.QUORUM)]
    [InlineData(QueueType.CLASSIC)]
    [InlineData(QueueType.STREAM)]
    public async Task DeclareQueueWithNoNameShouldGenerateClientSideName(QueueType type)
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Type(type);
        IQueueInfo queueInfo0 = await queueSpec.DeclareAsync();
        Assert.Contains("client.gen-", queueInfo0.Name());

        IQueueInfo queueInfo1 = await _management.GetQueueInfoAsync(queueSpec);
        Assert.Equal(queueInfo0.Name(), queueInfo1.Name());
        Assert.Equal((ulong)0, queueInfo1.MessageCount());

        await queueSpec.DeleteAsync();
    }

    /// <summary>
    /// Validate the queue declaration.
    /// The queue-info response should match the queue declaration.
    /// </summary>
    [Theory]
    [InlineData(true, false, false, QueueType.QUORUM)]
    [InlineData(true, false, false, QueueType.CLASSIC)]
    [InlineData(true, false, true, QueueType.CLASSIC)]
    [InlineData(true, true, true, QueueType.CLASSIC)]
    [InlineData(true, false, false, QueueType.STREAM)]
    public async Task DeclareQueueWithQueueInfoValidation(
        bool durable, bool autoDelete, bool exclusive, QueueType type)
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue()
            .Name(_queueName)
            .AutoDelete(autoDelete)
            .Exclusive(exclusive)
            .Type(type);
        IQueueInfo queueInfo = await queueSpec.DeclareAsync();

        Assert.Equal(_queueName, queueInfo.Name());
        Assert.Equal((ulong)0, queueInfo.MessageCount());
        Assert.Equal((uint)0, queueInfo.ConsumerCount());
        Assert.Equal(type, queueInfo.Type());

        if (type == QueueType.CLASSIC)
        {
            Assert.Single(queueInfo.Members());
        }
        else
        {
            Assert.Equal(ClusterSize, queueInfo.Members().Count);
        }

        Assert.NotNull(queueInfo.Leader());
        Assert.Equal(queueInfo.Durable(), durable);
        Assert.Equal(queueInfo.AutoDelete(), autoDelete);
        Assert.Equal(queueInfo.Exclusive(), exclusive);

        await queueSpec.DeleteAsync();
    }

    [Fact]
    public async Task DeclareQueueWithPreconditionFailedException()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec0 = _management.Queue().Name(_queueName).AutoDelete(false);
        await queueSpec0.DeclareAsync();

        // Re-declare, for kicks
        IQueueSpecification queueSpec1 = _management.Queue().Name(_queueName).AutoDelete(false);
        await queueSpec1.DeclareAsync();

        IQueueSpecification queueSpec3 = _management.Queue().Name(_queueName).AutoDelete(true);
        await Assert.ThrowsAsync<PreconditionFailedException>(() => queueSpec3.DeclareAsync());
    }

    [Fact]
    public async Task DeclareAndDeleteTwoTimesShouldNotRaiseErrors()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec1 = _management.Queue().Name(_queueName).AutoDelete(false);
        IQueueSpecification queueSpec2 = _management.Queue().Name(_queueName).AutoDelete(false);
        await WhenAllComplete(queueSpec1.DeclareAsync(), queueSpec2.DeclareAsync());
        await WhenAllComplete(queueSpec1.DeleteAsync(), queueSpec2.DeleteAsync());
    }

    [Fact]
    public async Task DeclareQueueWithArguments()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName)
            .DeadLetterExchange("my_exchange")
            .DeadLetterRoutingKey("my_key")
            .OverflowStrategy(OverFlowStrategy.DropHead)
            .MaxLengthBytes(ByteCapacity.Gb(1))
            .MaxLength(50000)
            .MessageTtl(TimeSpan.FromSeconds(10))
            .Expires(TimeSpan.FromSeconds(2))
            .SingleActiveConsumer(true);
        IQueueInfo queueInfo = await queueSpec.DeclareAsync();

        Assert.Equal(_queueName, queueInfo.Name());
        Assert.Equal("my_exchange", queueInfo.Arguments()["x-dead-letter-exchange"]);
        Assert.Equal("my_key", queueInfo.Arguments()["x-dead-letter-routing-key"]);
        Assert.Equal("drop-head", queueInfo.Arguments()["x-overflow"]);
        Assert.Equal(50000L, queueInfo.Arguments()["x-max-length"]);
        Assert.Equal(1000000000L, queueInfo.Arguments()["x-max-length-bytes"]);
        Assert.Equal(10000L, queueInfo.Arguments()["x-message-ttl"]);
        Assert.Equal(2000L, queueInfo.Arguments()["x-expires"]);
        Assert.Equal(true, queueInfo.Arguments()["x-single-active-consumer"]);
        // NB: DisposeAsync will delete the queue with name _queueName
    }

    [Fact]
    public async Task DeclareStreamQueueWithArguments()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueInfo queueInfo = await _management.Queue()
            .Name(_queueName)
            .MaxLengthBytes(ByteCapacity.Kb(1024))
            .LeaderLocator(LeaderLocatorStrategy.Balanced)
            .Stream()
            .MaxAge(TimeSpan.FromSeconds(10))
            .MaxSegmentSizeBytes(ByteCapacity.Kb(1024))
            .InitialClusterSize(1)
            .FileSizePerChunk(ByteCapacity.Kb(1024))
            .Queue()
            .DeclareAsync();

        Assert.Equal(_queueName, queueInfo.Name());
        Assert.Equal("10s", queueInfo.Arguments()["x-max-age"]);
        Assert.Equal(1024000L, queueInfo.Arguments()["x-stream-max-segment-size-bytes"]);
        Assert.Equal(1, queueInfo.Arguments()["x-initial-cluster-size"]);
        Assert.Equal("balanced", queueInfo.Arguments()["x-queue-leader-locator"]);
        Assert.Equal(1024000L, queueInfo.Arguments()["x-stream-file-size-per-chunk"]);
        Assert.Equal(1024000L, queueInfo.Arguments()["x-max-length-bytes"]);
        // NB: DisposeAsync will delete the queue with name _queueName
    }

    [Fact]
    public async Task DeclareQuorumQueueWithArguments()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueInfo queueInfo = await _management.Queue()
            .Name(_queueName)
            .LeaderLocator(LeaderLocatorStrategy.ClientLocal)
            .Quorum()
            .DeliveryLimit(12)
            .DeadLetterStrategy(QuorumQueueDeadLetterStrategy.AtLeastOnce)
            .QuorumInitialGroupSize(3)
            .QuorumTargetGroupSize(5)
            .Queue()
            .DeclareAsync();

        Assert.Equal(_queueName, queueInfo.Name());
        Assert.Equal(12, queueInfo.Arguments()["x-max-delivery-limit"]);
        Assert.Equal("at-least-once", queueInfo.Arguments()["x-dead-letter-strategy"]);
        Assert.Equal(3, queueInfo.Arguments()["x-quorum-initial-group-size"]);
        Assert.Equal(5, queueInfo.Arguments()["x-quorum-target-group-size"]);
        Assert.Equal("client-local", queueInfo.Arguments()["x-queue-leader-locator"]);
        // NB: DisposeAsync will delete the queue with name _queueName
    }

    [Fact]
    public async Task DeclareClassicQueueWithArguments()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueInfo info = await _management.Queue()
            .Name(_queueName)
            .Classic()
            .Mode(ClassicQueueMode.Lazy)
            .Version(ClassicQueueVersion.V2)
            .Queue()
            .DeclareAsync();

        Assert.Equal("lazy", info.Arguments()["x-queue-mode"]);
        Assert.Equal(2, info.Arguments()["x-queue-version"]);
        // NB: DisposeAsync will delete the queue with name _queueName
    }

    [Fact]
    public async Task DeclareQueueWithUnsupportedArgument()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpecification0 = _management.Queue(_queueName).Type(QueueType.CLASSIC);
        Dictionary<object, object> queueSpec0Args = queueSpecification0.QueueArguments;
        queueSpec0Args.Add("x-max-age", "1000s");
        PreconditionFailedException pex0 =
            await Assert.ThrowsAsync<PreconditionFailedException>(queueSpecification0.DeclareAsync);
        Assert.Contains("409", pex0.Message);

        IQueueSpecification queueSpecification1 = _management.Queue(_queueName).Type(QueueType.QUORUM);
        Dictionary<object, object> queueSpec1Args = queueSpecification1.QueueArguments;
        queueSpec1Args.Add("x-max-age", "1000s");
        PreconditionFailedException pex1 =
            await Assert.ThrowsAsync<PreconditionFailedException>(queueSpecification1.DeclareAsync);
        Assert.Contains("409", pex1.Message);

        IQueueSpecification queueSpecification2 = _management.Queue(_queueName).Type(QueueType.STREAM);
        queueSpecification2.DeadLetterRoutingKey("not-supported");
        PreconditionFailedException pex2 =
            await Assert.ThrowsAsync<PreconditionFailedException>(queueSpecification2.DeclareAsync);
        Assert.Contains("409", pex2.Message);
    }

    [Fact]
    public async Task ValidateDeclareQueueArguments()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            _management.Queue().Name(_queueName).MessageTtl(TimeSpan.FromSeconds(-1))
                .DeclareAsync());

        await Assert.ThrowsAsync<ArgumentException>(() =>
            _management.Queue().Name(_queueName).Expires(TimeSpan.FromSeconds(0))
                .DeclareAsync());

        await Assert.ThrowsAsync<ArgumentException>(() =>
            _management.Queue().Name(_queueName).MaxLengthBytes(ByteCapacity.Gb(-1))
                .DeclareAsync());

        await Assert.ThrowsAsync<ArgumentException>(() =>
            _management.Queue().Name(_queueName).MaxLength(-1).DeclareAsync());

        await Assert.ThrowsAsync<ArgumentException>(() =>
            _management.Queue().Name(_queueName).Stream().InitialClusterSize(-1)
                .Queue().DeclareAsync());

        await Assert.ThrowsAsync<ArgumentException>(() =>
            _management.Queue().Name(_queueName).Stream()
                .MaxSegmentSizeBytes(ByteCapacity.Gb(-1))
                .Queue().DeclareAsync());

        await Assert.ThrowsAsync<ArgumentException>(() =>
            _management.Queue().Name(_queueName).Stream()
                .MaxAge(TimeSpan.FromSeconds(-1))
                .Queue().DeclareAsync());

        await Assert.ThrowsAsync<ArgumentException>(() =>
            _management.Queue().Name(_queueName).Quorum()
                .DeliveryLimit(-1)
                .Queue().DeclareAsync());
    }

    /// <summary>
    /// Simple test to declare an exchange with the default values.
    /// </summary>
    [Fact]
    public async Task SimpleDeclareAndDeleteExchangeWithName()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IExchangeSpecification exchangeSpec = _management.Exchange(_exchangeName).Type(ExchangeType.TOPIC);
        await exchangeSpec.DeclareAsync();

        await _management.Exchange(_exchangeName).Type(ExchangeType.TOPIC).DeclareAsync();

        await WaitUntilExchangeExistsAsync(exchangeSpec);

        await exchangeSpec.DeleteAsync();

        await WaitUntilExchangeDeletedAsync(exchangeSpec);
    }

    [Fact]
    public async Task DeclareAndDeleteExchangeWithStringType()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IExchangeSpecification exchangeSpec = _management.Exchange(_exchangeName).Type("direct");
        await exchangeSpec.DeclareAsync();

        await _management.Exchange(_exchangeName).Type("direct").DeclareAsync();

        await WaitUntilExchangeExistsAsync(exchangeSpec);

        await exchangeSpec.DeleteAsync();

        await WaitUntilExchangeDeletedAsync(exchangeSpec);
    }

    [Fact]
    public async Task ExchangeWithEmptyNameShouldRaiseAnException()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            _management.Exchange("").Type(ExchangeType.TOPIC).DeclareAsync());
    }

    [Fact]
    public async Task RedeclareExchangeWithSameArgs()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IExchangeSpecification exchangeSpec0 =
            _management.Exchange(_exchangeName).AutoDelete(true).Argument("my_key", "my _value");
        await exchangeSpec0.DeclareAsync();

        // Second re-declare
        IExchangeSpecification exchangeSpec1 =
            _management.Exchange(_exchangeName).AutoDelete(true).Argument("my_key", "my _value");
        await exchangeSpec1.DeclareAsync();

        await WaitUntilExchangeExistsAsync(exchangeSpec0);

        await exchangeSpec0.DeleteAsync();
        await WaitUntilExchangeDeletedAsync(exchangeSpec0);
    }

    [Fact]
    public async Task RedeclareExchangeWithDifferentArgs()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IExchangeSpecification exchangeSpec0 = _management.Exchange(_exchangeName).Type(ExchangeType.DIRECT);
        await exchangeSpec0.DeclareAsync();

        IExchangeSpecification exchangeSpec1 = _management.Exchange(_exchangeName).Type(ExchangeType.FANOUT);

        PreconditionFailedException pfex =
            await Assert.ThrowsAsync<PreconditionFailedException>(exchangeSpec1.DeclareAsync);

        await exchangeSpec0.DeleteAsync();
        await WaitUntilExchangeDeletedAsync(exchangeSpec0);
    }

    [Fact]
    public async Task DeclareExchangeWithPreconditionFailedException()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IExchangeSpecification exchangeSpecification = _management.Exchange(_exchangeName)
            .AutoDelete(true)
            .Argument("my_key", "my _value");
        await exchangeSpecification.DeclareAsync();

        await Assert.ThrowsAsync<PreconditionFailedException>(async () =>
            await _management.Exchange(_exchangeName).AutoDelete(false)
                .Argument("my_key_2", "my _value_2").DeclareAsync());

        await WaitUntilExchangeExistsAsync(exchangeSpecification);

        await exchangeSpecification.DeleteAsync();
        await WaitUntilExchangeDeletedAsync("my_exchange_raise_precondition_fail");
    }

    ////////////// ----------------- Topology TESTS ----------------- //////////////

    /// <summary>
    /// Validate the topology listener.
    /// The listener should be able to record the queue declaration.
    /// creation and deletion.
    /// </summary>
    [Fact]
    public async Task TopologyCountShouldFollowTheQueueDeclaration()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);
        ITopologyListener topologyListener = ((IManagementTopology)_management).TopologyListener();

        var queueSpecs = new List<IQueueSpecification>();
        for (int i = 1; i < 7; i++)
        {
            IQueueSpecification qs = _management.Queue().Name($"{_queueName}_{i}");
            await qs.DeclareAsync();
            queueSpecs.Add(qs);
            Assert.Equal(((RecordingTopologyListener)topologyListener).QueueCount(), i);
        }

        for (int i = 0; i < 6; i++)
        {
            IQueueSpecification qs = queueSpecs[i];
            await qs.DeleteAsync();
            Assert.Equal(topologyListener.QueueCount(), 5 - i);
        }

        queueSpecs.Clear();
    }

    [Theory]
    [InlineData(QueueType.QUORUM)]
    [InlineData(QueueType.CLASSIC)]
    public async Task PurgeQueueShouldReturnTheCorrectNumberOfMessagesPurged(QueueType queueType)
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName).Type(queueType);
        await queueSpec.DeclareAsync();
        await PublishAsync(queueSpec, 19);
        await WaitUntilQueueMessageCount(_queueName, 19);
        ulong deleted = await queueSpec.PurgeAsync();
        Assert.Equal((ulong)19, deleted);
        await WaitUntilQueueMessageCount(_queueName, 0);
    }

    [Fact]
    public async Task PurgeQueueShouldReturnErrorForStream()
    {
        Assert.NotNull(_connection);
        Assert.NotNull(_management);

        IQueueSpecification queueSpec = _management.Queue().Name(_queueName).Type(QueueType.STREAM);
        await queueSpec.DeclareAsync();
        await PublishAsync(queueSpec, 19);
        await WaitUntilQueueMessageCount(_queueName, 19);
        await Assert.ThrowsAsync<BadRequestException>(queueSpec.PurgeAsync);
    }
}
