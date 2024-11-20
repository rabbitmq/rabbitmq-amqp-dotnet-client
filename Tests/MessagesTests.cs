// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;

namespace Tests;

public class MessagesTests
{
    [Fact]
    public void ValidateMessage()
    {
        IMessage message = new AmqpMessage("my_body")
            .MessageId("MessageId_123")
            .CorrelationId("CorrelationId_2123")
            .ReplyTo("ReplyTo_5123")
            .Subject("Subject_9123");
        Assert.Equal("MessageId_123", message.MessageId());
        Assert.Equal("CorrelationId_2123", message.CorrelationId());
        Assert.Equal("ReplyTo_5123", message.ReplyTo());
        Assert.Equal("Subject_9123", message.Subject());
        Assert.Equal("my_body", message.Body());
    }

    [Fact]
    public void ThrowExceptionIfPropertiesNotSet()
    {
        IMessage message = new AmqpMessage("my_body");
        Assert.Throws<FieldNotSetException>(() => message.MessageId());
        Assert.Throws<FieldNotSetException>(() => message.Subject());
        Assert.Throws<FieldNotSetException>(() => message.CorrelationId());
        Assert.Throws<FieldNotSetException>(() => message.ReplyTo());
    }

}
