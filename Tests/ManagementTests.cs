using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using RabbitMQ.AMQP.Client;
using Xunit;
using Message = Amqp.Message;

namespace Tests;

internal class TestAmqpManagement : AmqpManagement
{
    protected override async Task InternalSendAsync(Message message)
    {
        await Task.Delay(1000);
    }

    public void TestHandleResponseMessage(Message msg)
    {
        base.HandleResponseMessage(msg);
    }
}

public class ManagementTests()
{
    [Fact]
    public async Task RaiseTaskCanceledException()
    {
        var management = new TestAmqpManagement();
        management.SetOpen();
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
    public async Task RaiseInvalidOperationException()
    {
        var management = new TestAmqpManagement();
        management.SetOpen();
        
        const string messageId = "my_id";
        var t = Task.Run(async () =>
        {
            await Task.Delay(1000);
            management.TestHandleResponseMessage(new Message()
            {
                Properties = new Properties()
                {
                    CorrelationId = messageId,
                    Subject = "Not_a_Number",
                }
            });
        });

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await management.Request(messageId, "", "", "", 
                [111]));
    

        await t.WaitAsync(TimeSpan.FromMilliseconds(1000));
        await management.CloseAsync();
    }
    
    
    [Fact]
    public async Task RaiseInvalidCodeException()
    {
        var management = new TestAmqpManagement();
        management.SetOpen();
        
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
        Assert.Equal(ManagementStatus.Closed, management.Status);
    }

    [Fact]
    public async Task RaiseManagementClosedException()
    {
        var management = new TestAmqpManagement();
        await Assert.ThrowsAsync<ManagementClosedException>(async () =>
            await management.Request(new Message(), [200]));
        Assert.Equal(ManagementStatus.Closed, management.Status);
    }


    [Fact]
    public async void DeclareFirstQueue()
    {
        AmqpConnection connection = new();
        await connection.ConnectAsync(new AmqpAddress("localhost", 5672));
        var management = connection.Management();
        await management.Queue().Name("dot_test_1").Durable(true).Declare();
        await connection.CloseAsync();
        Assert.Equal(ManagementStatus.Closed, management.Status);
        
    }


    // [Fact]
    // public void DeclareQueue()
    // {
    //     var connection = new Connection(
    //         new Address("amqp://localhost:5672"));
    //     var managementSession = new Session(connection);
    //
    //
    //     var senderAttach = new Attach
    //     {
    //         SndSettleMode = SenderSettleMode.Settled,
    //         RcvSettleMode = ReceiverSettleMode.First,
    //
    //         Properties = new Fields
    //         {
    //             { new Symbol("paired"), true }
    //         },
    //         LinkName = "management-link-pair",
    //         Source = new Source()
    //         {
    //             Address = "/management",
    //             ExpiryPolicy = new Symbol("LINK_DETACH"),
    //             Timeout = 0,
    //             Dynamic = false,
    //             Durable = 0
    //         },
    //         Handle = 0,
    //         Target = new Target()
    //         {
    //             Address = "/management",
    //             ExpiryPolicy = new Symbol("SESSION_END"),
    //             Timeout = 0,
    //             Dynamic = false,
    //         },
    //     };
    //
    //     var receiveAttach = new Attach()
    //     {
    //         SndSettleMode = SenderSettleMode.Settled,
    //         RcvSettleMode = ReceiverSettleMode.First,
    //         Properties = new Fields
    //         {
    //             { new Symbol("paired"), true }
    //         },
    //         LinkName = "management-link-pair",
    //         Source = new Source()
    //         {
    //             Address = "/management",
    //             ExpiryPolicy = new Symbol("LINK_DETACH"),
    //             Timeout = 0,
    //             Dynamic = false,
    //             Durable = 0,
    //         },
    //         Handle = 1,
    //         Target = new Target()
    //         {
    //             Address = "/management",
    //             ExpiryPolicy = new Symbol("SESSION_END"),
    //             Timeout = 0,
    //             Dynamic = false,
    //         },
    //     };
    //
    //     var sender = new SenderLink(
    //         managementSession, "management-link-pair", senderAttach, null);
    //
    //     var receiver = new ReceiverLink(
    //         managementSession, "management-link-pair", receiveAttach, null);
    //
    //     receiver.Start(
    //         20,
    //         (link, messageR) => { link.Accept(messageR); });
    //     Task.Run(() =>
    //     {
    //         while (true)
    //         {
    //             var msgRecv = receiver.Receive();
    //             receiver.Accept(msgRecv);
    //         }
    //     });
    //     Thread.Sleep(500);
    //     var kv = new Map
    //     {
    //         { "durable", true },
    //         { "exclusive", false },
    //         { "auto_delete", false }
    //     };
    //     var message = new Message(kv);
    //     message.Properties = new Properties
    //     {
    //         MessageId = "0",
    //         To = "/queues/test",
    //         Subject = "PUT",
    //         ReplyTo = "$me"
    //     };
    //
    //     sender.Send(message);
    //
    //     sender.Close();
    //     managementSession.Close();
    // }
}