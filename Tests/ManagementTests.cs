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

public class ManagementTests()
{
    [Fact]
    public async void DeclareFirstQueue()
    {
        AmqpConnection connection = new();
        await connection.ConnectAsync(new AmqpAddress("localhost", 5672));
        var management = connection.Management();
        await management.Queue().Name("dot_test").Durable(true).Declare();
        await connection.CloseAsync();
    }


    [Fact]
    public void DeclareQueue()
    {
        var connection = new Connection(
            new Address("amqp://localhost:5672"));
        var managementSession = new Session(connection);


        var senderAttach = new Attach
        {
            SndSettleMode = SenderSettleMode.Settled,
            RcvSettleMode = ReceiverSettleMode.First,

            Properties = new Fields
            {
                { new Symbol("paired"), true }
            },
            LinkName = "management-link-pair",
            Source = new Source()
            {
                Address = "/management",
                ExpiryPolicy = new Symbol("LINK_DETACH"),
                Timeout = 0,
                Dynamic = false,
                Durable = 0
            },
            Handle = 0,
            Target = new Target()
            {
                Address = "/management",
                ExpiryPolicy = new Symbol("SESSION_END"),
                Timeout = 0,
                Dynamic = false,
            },
        };

        var receiveAttach = new Attach()
        {
            SndSettleMode = SenderSettleMode.Settled,
            RcvSettleMode = ReceiverSettleMode.First,
            Properties = new Fields
            {
                { new Symbol("paired"), true }
            },
            LinkName = "management-link-pair",
            Source = new Source()
            {
                Address = "/management",
                ExpiryPolicy = new Symbol("LINK_DETACH"),
                Timeout = 0,
                Dynamic = false,
                Durable = 0,
            },
            Handle = 1,
            Target = new Target()
            {
                Address = "/management",
                ExpiryPolicy = new Symbol("SESSION_END"),
                Timeout = 0,
                Dynamic = false,
            },
        };

        var sender = new SenderLink(
            managementSession, "management-link-pair", senderAttach, null);

        var receiver = new ReceiverLink(
            managementSession, "management-link-pair", receiveAttach, null);

        receiver.Start(
            20,
            (link, messageR) => { link.Accept(messageR); });
        Task.Run(() =>
        {
            while (true)
            {
                var msgRecv = receiver.Receive();
                receiver.Accept(msgRecv);
            }
        });
        Thread.Sleep(500);
        var kv = new Map
        {
            { "durable", true },
            { "exclusive", false },
            { "auto_delete", false }
        };
        var message = new Message(kv);
        message.Properties = new Properties
        {
            MessageId = "0",
            To = "/queues/test",
            Subject = "PUT",
            ReplyTo = "$me"
        };

        sender.Send(message);

        sender.Close();
        managementSession.Close();
    }
}