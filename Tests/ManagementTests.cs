using System;
using System.Threading.Tasks;
using Amqp.Framing;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Message = Amqp.Message;

namespace Tests;

internal class TestAmqpManagement() : AmqpManagement(new AmqpManagementParameters(null!))
{
    protected override async Task InternalSendAsync(Message message)
    {
        await Task.Delay(1000);
    }
}

internal class TestAmqpManagementOpen : AmqpManagement
{
    public TestAmqpManagementOpen() : base(new AmqpManagementParameters(null!))
    {
        State = State.Open;
    }

    protected override async Task InternalSendAsync(Message message)
    {
        await Task.Delay(1000);
    }

    public void TestHandleResponseMessage(Message msg)
    {
        HandleResponseMessage(msg);
    }
}

public class ManagementTests()
{
    [Fact]
    public async Task RaiseTaskCanceledException()
    {
        var management = new TestAmqpManagementOpen();
        var message = new Message() { Properties = new Properties() { MessageId = "a_random_id", } };
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
            await management.Request(message, [200], TimeSpan.FromSeconds(1)));
        await management.CloseAsync();
    }

    /// <summary>
    /// Test to raise a ModelException based on checking the response
    /// the message _must_ respect the following rules:
    /// - id and correlation id should match
    /// - subject _must_ be a number
    /// The test validate the following cases:
    ///  - subject is not a number
    ///  - code is not in the expected list
    ///  - correlation id is not the same as the message id
    /// </summary>
    [Fact]
    public void RaiseModelException()
    {
        var management = new TestAmqpManagement();
        const string messageId = "my_id";
        var sent = new Message() { Properties = new Properties() { MessageId = messageId, } };

        var receive = new Message()
        {
            Properties = new Properties() { CorrelationId = messageId, Subject = "Not_a_Number", }
        };

        Assert.Throws<ModelException>(() =>
            management.CheckResponse(sent, [], receive));

        receive.Properties.Subject = "200";
        Assert.Throws<InvalidCodeException>(() =>
            management.CheckResponse(sent, [201], receive));

        receive.Properties.CorrelationId = "not_my_id";
        Assert.Throws<ModelException>(() =>
            management.CheckResponse(sent, [], receive));
    }

    [Fact]
    public async Task RaiseInvalidCodeException()
    {
        var management = new TestAmqpManagementOpen();

        const string messageId = "my_id";
        var t = Task.Run(async () =>
        {
            await Task.Delay(1000);
            management.TestHandleResponseMessage(new Message()
            {
                Properties = new Properties()
                {
                    CorrelationId = messageId,
                    Subject = "506", // 506 is not a valid code
                }
            });
        });

        await Assert.ThrowsAsync<InvalidCodeException>(async () =>
            await management.Request(messageId, "", "", "",
                [200]));

        await t.WaitAsync(TimeSpan.FromMilliseconds(1000));
        await management.CloseAsync();
    }

    [Fact]
    public async Task RaiseManagementClosedException()
    {
        var management = new TestAmqpManagement();
        await Assert.ThrowsAsync<AmqpNotOpenException>(async () =>
           await management.Request(new Message(), [200]));
        Assert.Equal(State.Closed, management.State);
    }


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
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        IQueueInfo queueInfo = await management.Queue().Type(type).Declare();
        Assert.Contains("client.gen-", queueInfo.Name());
        await management.QueueDeletion().Delete(queueInfo.Name());
        await connection.CloseAsync();
        Assert.Equal(State.Closed, management.State);
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
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        var queueInfo = await management.Queue().Name("validate_queue_info").AutoDelete(autoDelete).Exclusive(exclusive)
            .Type(type)
            .Declare();
        Assert.Equal("validate_queue_info", queueInfo.Name());
        Assert.Equal((ulong)0, queueInfo.MessageCount());
        Assert.Equal((uint)0, queueInfo.ConsumerCount());
        Assert.Equal(type, queueInfo.Type());
        Assert.Single(queueInfo.Replicas());
        Assert.NotNull(queueInfo.Leader());
        Assert.Equal(queueInfo.Durable(), durable);
        Assert.Equal(queueInfo.AutoDelete(), autoDelete);
        Assert.Equal(queueInfo.Exclusive(), exclusive);
        await management.QueueDeletion().Delete("validate_queue_info");
        await connection.CloseAsync();
        Assert.Equal(State.Closed, management.State);
    }


    [Fact]
    public async Task DeclareQueueWithPreconditionFailedException()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        var management = connection.Management();
        await management.Queue().Name("precondition_queue_fail").AutoDelete(false).Declare();
        await Assert.ThrowsAsync<PreconditionFailedException>(async () =>
            await management.Queue().Name("precondition_queue_fail").AutoDelete(true).Declare());
        await management.QueueDeletion().Delete("precondition_queue_fail");
        await connection.CloseAsync();
    }


    [Fact]
    public async Task DeclareAndDeleteTwoTimesShouldNotRaiseErrors()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        var management = connection.Management();
        await management.Queue().Name("DeleteTwoTimes").AutoDelete(false).Declare();
        await management.Queue().Name("DeleteTwoTimes").AutoDelete(false).Declare();
        await management.QueueDeletion().Delete("DeleteTwoTimes");
        await management.QueueDeletion().Delete("DeleteTwoTimes");
        await connection.CloseAsync();
    }


    [Fact]
    public async Task DeclareQueueWithDifferentArguments()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        var management = connection.Management();

        IQueueInfo q = await management.Queue().Name("DeclareQueueWithDifferentArguments")
            .DeadLetterExchange("my_exchange")
            .DeadLetterRoutingKey("my_key").OverflowStrategy(OverFlowStrategy.DropHead)
            .MaxLengthBytes(ByteCapacity.Gb(1)).MaxLength(50000).MessageTtl(TimeSpan.FromSeconds(10))
            .Expires(TimeSpan.FromSeconds(2)).SingleActiveConsumer(true).Declare();

        Assert.Equal("DeclareQueueWithDifferentArguments", q.Name());
        Assert.Equal("my_exchange", q.Arguments()["x-dead-letter-exchange"]);
        Assert.Equal("my_key", q.Arguments()["x-dead-letter-routing-key"]);
        Assert.Equal("drop-head", q.Arguments()["x-overflow"]);
        Assert.Equal(50000L, q.Arguments()["x-max-length"]);
        Assert.Equal(1000000000L, q.Arguments()["x-max-length-bytes"]);
        Assert.Equal(10000L, q.Arguments()["x-message-ttl"]);
        Assert.Equal(2000L, q.Arguments()["x-expires"]);
        Assert.Equal(true, q.Arguments()["x-single-active-consumer"]);

        await management.QueueDeletion().Delete("DeclareQueueWithDifferentArguments");
        await connection.CloseAsync();
    }


    [Fact]
    public async Task DeclareStreamQueueWithDifferentArguments()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        var management = connection.Management();

        IQueueInfo info = await management.Queue().Name("DeclareStreamQueueWithDifferentArguments")
            .Stream().MaxAge(TimeSpan.FromSeconds(10)).MaxSegmentSizeBytes(ByteCapacity.Kb(1024)).InitialClusterSize(1)
            .Queue()
            .Declare();

        Assert.Equal("DeclareStreamQueueWithDifferentArguments", info.Name());
        Assert.Equal("10s", info.Arguments()["x-max-age"]);
        Assert.Equal(1024000L, info.Arguments()["x-stream-max-segment-size-bytes"]);
        Assert.Equal(1, info.Arguments()["x-initial-cluster-size"]);
        await management.QueueDeletion().Delete("DeclareStreamQueueWithDifferentArguments");
        await connection.CloseAsync();
    }


    [Fact]
    public async Task DeclareQuorumQueueWithDifferentArguments()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        var management = connection.Management();
        IQueueInfo info = await management.Queue().Name("DeclareQuorumQueueWithDifferentArguments")
            .Quorum()
            .DeliveryLimit(12)
            .DeadLetterStrategy(QuorumQueueDeadLetterStrategy.AtLeastOnce)
            .QuorumInitialGroupSize(3)
            .Queue()
            .Declare();

        Assert.Equal("DeclareQuorumQueueWithDifferentArguments", info.Name());
        Assert.Equal(12, info.Arguments()["x-max-delivery-limit"]);
        Assert.Equal("at-least-once", info.Arguments()["x-dead-letter-strategy"]);
        Assert.Equal(3, info.Arguments()["x-quorum-initial-group-size"]);
        await management.QueueDeletion().Delete("DeclareQuorumQueueWithDifferentArguments");
        await connection.CloseAsync();
    }

    [Fact]
    public async Task DeclareClassicQueueWithDifferentArguments()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        var management = connection.Management();
        IQueueInfo info = await management.Queue().Name("DeclareClassicQueueWithDifferentArguments")
            .Classic()
            .Mode(ClassicQueueMode.Lazy)
            .Version(ClassicQueueVersion.V2)
            .Queue()
            .Declare();

        Assert.Equal("DeclareClassicQueueWithDifferentArguments", info.Name());
        Assert.Equal("lazy", info.Arguments()["x-queue-mode"]);
        Assert.Equal(2, info.Arguments()["x-queue-version"]);
        await management.QueueDeletion().Delete("DeclareClassicQueueWithDifferentArguments");
        await connection.CloseAsync();
    }


    [Fact]
    public async Task ValidateDeclareQueueArguments()
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();

        await Assert.ThrowsAsync<ArgumentException>(() =>
            management.Queue().Name("ValidateDeclareQueueWithDifferentArguments").MessageTtl(TimeSpan.FromSeconds(-1))
                .Declare());


        await Assert.ThrowsAsync<ArgumentException>(() =>
            management.Queue().Name("ValidateDeclareQueueWithDifferentArguments").Expires(TimeSpan.FromSeconds(0))
                .Declare());

        await Assert.ThrowsAsync<ArgumentException>(() =>
            management.Queue().Name("ValidateDeclareQueueWithDifferentArguments").MaxLengthBytes(ByteCapacity.Gb(-1))
                .Declare());


        await Assert.ThrowsAsync<ArgumentException>(() =>
            management.Queue().Name("ValidateDeclareQueueWithDifferentArguments").MaxLength(-1).Declare());


        await Assert.ThrowsAsync<ArgumentException>(() =>
            management.Queue().Name("ValidateDeclareQueueWithDifferentArguments").Stream().InitialClusterSize(-1)
                .Queue().Declare());


        await Assert.ThrowsAsync<ArgumentException>(() =>
            management.Queue().Name("ValidateDeclareQueueWithDifferentArguments").Stream()
                .MaxSegmentSizeBytes(ByteCapacity.Gb(-1))
                .Queue().Declare());


        await Assert.ThrowsAsync<ArgumentException>(() =>
            management.Queue().Name("ValidateDeclareQueueWithDifferentArguments").Stream()
                .MaxAge(TimeSpan.FromSeconds(-1))
                .Queue().Declare());

        await Assert.ThrowsAsync<ArgumentException>(() =>
            management.Queue().Name("ValidateDeclareQueueWithDifferentArguments").Quorum()
                .DeliveryLimit(-1)
                .Queue().Declare());

        await connection.CloseAsync();
    }


    ////////////// ----------------- Exchanges TESTS ----------------- //////////////


    /// <summary>
    /// Simple test to declare an exchange with the default values.
    /// </summary>
    [Fact]
    public async Task SimpleDeclareAndDeleteExchangeWithName()
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        await management.Exchange("my_first_exchange").Type(ExchangeType.TOPIC).Declare();
        SystemUtils.WaitUntil(() => SystemUtils.ExchangeExists("my_first_exchange"));
        await management.ExchangeDeletion().Delete("my_first_exchange");
        SystemUtils.WaitUntil(() => SystemUtils.ExchangeExists("my_first_exchange") == false);
        await connection.CloseAsync();
    }


    [Fact]
    public async Task ExchangeWithEmptyNameShouldRaiseAnException()
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        await Assert.ThrowsAsync<ArgumentException>(() => management.Exchange("").Type(ExchangeType.TOPIC).Declare());
        await connection.CloseAsync();
    }

    [Fact]
    public async Task ExchangeWithDifferentArgs()
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        await management.Exchange("my_exchange_with_args").AutoDelete(true).Argument("my_key", "my _value").Declare();
        SystemUtils.WaitUntil(() => SystemUtils.ExchangeExists("my_exchange_with_args"));
        await management.ExchangeDeletion().Delete("my_exchange_with_args");
        await connection.CloseAsync();
        SystemUtils.WaitUntil(() => !SystemUtils.ExchangeExists("my_exchange_with_args"));
    }


    [Fact]
    public async Task DeclareExchangeWithPreconditionFailedException()
    {
        IConnection connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        IManagement management = connection.Management();
        await management.Exchange("my_exchange_raise_precondition_fail").AutoDelete(true)
            .Argument("my_key", "my _value").Declare();
        await Assert.ThrowsAsync<PreconditionFailedException>(async () =>
            await management.Exchange("my_exchange_raise_precondition_fail").AutoDelete(false)
                .Argument("my_key_2", "my _value_2").Declare());
        SystemUtils.WaitUntil(() => SystemUtils.ExchangeExists("my_exchange_raise_precondition_fail"));
        await management.ExchangeDeletion().Delete("my_exchange_raise_precondition_fail");
        await connection.CloseAsync();
        SystemUtils.WaitUntil(() => !SystemUtils.ExchangeExists("my_exchange_raise_precondition_fail"));
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
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        var management = connection.Management();
        for (int i = 1; i < 7; i++)
        {
            await management.Queue().Name($"Q_{i}").Declare();
            Assert.Equal(((RecordingTopologyListener)management.TopologyListener()).QueueCount(), i);
        }

        for (int i = 1; i < 7; i++)
        {
            await management.QueueDeletion().Delete($"Q_{i}");
            Assert.Equal((management.TopologyListener()).QueueCount(), 6 - i);
        }

        await connection.CloseAsync();
    }
}
