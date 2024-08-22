// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading.Tasks;
using Amqp.Framing;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Message = Amqp.Message;

namespace Tests;

public class MockManagementTests()
{
    public class TestAmqpManagement() : AmqpManagement(new AmqpManagementParameters(null!))
    {
        protected override async Task InternalSendAsync(Message message, TimeSpan timeout)
        {
            await Task.Delay(1000);
        }
    }

    public class TestAmqpManagementOpen : AmqpManagement
    {
        public TestAmqpManagementOpen() : base(new AmqpManagementParameters(null!))
        {
            State = State.Open;
        }

        protected override async Task InternalSendAsync(Message message, TimeSpan timeout)
        {
            await Task.Delay(1000);
        }

        public void TestHandleResponseMessage(Message msg)
        {
            HandleResponseMessage(msg);
        }
    }

    [Fact]
    public async Task RaiseTaskCanceledException()
    {
        var management = new TestAmqpManagementOpen();
        var message = new Message() { Properties = new Properties() { MessageId = "a_random_id", } };
        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
            await management.RequestAsync(message, [200], TimeSpan.FromSeconds(1)));
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
            await management.RequestAsync(messageId, "", "", "",
                [200]));

        await t.WaitAsync(TimeSpan.FromMilliseconds(1000));
        await management.CloseAsync();
    }

    [Fact]
    public async Task RaiseManagementClosedException()
    {
        var management = new TestAmqpManagement();
        await Assert.ThrowsAsync<AmqpNotOpenException>(async () =>
           await management.RequestAsync(new Message(), [200]));
        Assert.Equal(State.Closed, management.State);
    }
}
