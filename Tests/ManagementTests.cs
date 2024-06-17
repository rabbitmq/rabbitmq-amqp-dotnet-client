using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amqp.Framing;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Message = Amqp.Message;

namespace Tests;

internal class TestAmqpManagement : AmqpManagement
{
    protected override async Task InternalSendAsync(Message message)
    {
        await Task.Delay(1000);
    }
}

internal class TestAmqpManagementOpen : AmqpManagement
{
    protected override async Task InternalSendAsync(Message message)
    {
        await Task.Delay(1000);
    }

    public void TestHandleResponseMessage(Message msg)
    {
        HandleResponseMessage(msg);
    }

    public override Status Status { get; protected set; } = Status.Open;
}

public class ManagementTests()
{
    [Fact]
    public async Task RaiseTaskCanceledException()
    {
        var management = new TestAmqpManagementOpen();
        var message = new Message()
        {
            Properties = new Properties()
            {
                MessageId = "a_random_id",
            }
        };
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
            await management.Request(message, [200], TimeSpan.FromSeconds(1)));
        await management.CloseAsync();
    }

    [Fact]
    public void RaiseModelException()
    {
        var management = new TestAmqpManagement();

        const string messageId = "my_id";

        var sent = new Message()
        {
            Properties = new Properties()
            {
                MessageId = messageId,
            }
        };


        var receive = new Message()
        {
            Properties = new Properties()
            {
                CorrelationId = messageId,
                Subject = "Not_a_Number",
            }
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
        await Assert.ThrowsAsync<ModelException>(async () =>
            await management.Request(new Message(), [200]));
        Assert.Equal(Status.Closed, management.Status);
    }


    [Theory]
    [InlineData(QueueType.QUORUM)]
    [InlineData(QueueType.CLASSIC)]
    [InlineData(QueueType.STREAM)]
    public async void DeclareQueueWithNoNameShouldGenerateClientSideName(QueueType type)
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        await connection.ConnectAsync();
        var management = connection.Management();
        var queueInfo = await management.Queue().Type(type).Declare();
        Assert.Contains("client.gen-", queueInfo.Name());
        await management.QueueDeletion().Delete(queueInfo.Name());
        await connection.CloseAsync();
        Assert.Equal(Status.Closed, management.Status);
    }

    [Theory]
    [InlineData(true, false, false, QueueType.QUORUM)]
    [InlineData(true, false, false, QueueType.CLASSIC)]
    [InlineData(true, false, true, QueueType.CLASSIC)]
    [InlineData(true, true, true, QueueType.CLASSIC)]
    [InlineData(true, false, false, QueueType.STREAM)]
    public async void DeclareQueueWithQueueInfoValidation(
        bool durable, bool autoDelete, bool exclusive, QueueType type)
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        await connection.ConnectAsync();
        var management = connection.Management();
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
        Assert.Equal(Status.Closed, management.Status);
    }

    [Fact]
    public async void TopologyCountShouldFollowTheQueueDeclaration()
    {
        var connection = await AmqpConnection.CreateAsync(ConnectionSettingBuilder.Create().Build());
        await connection.ConnectAsync();
        var management = connection.Management();
        for (var i = 1; i < 7; i++)
        {
            await management.Queue().Name($"Q_{i}").Declare();
            Assert.Equal(((RecordingTopologyListener)management.TopologyListener()).QueueCount(), i);
        }

        for (var i = 1; i < 7; i++)
        {
            await management.QueueDeletion().Delete($"Q_{i}");
            Assert.Equal(((RecordingTopologyListener)management.TopologyListener()).QueueCount(), 6 - i);
        }

        await connection.CloseAsync();
    }
}