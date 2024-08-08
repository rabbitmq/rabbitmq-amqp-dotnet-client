// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class DeliveryContext(IReceiverLink link, Message message, UnsettledMessageCounter unsettledMessageCounter) : IContext
{
    public void Accept()
    {
        if (link.IsClosed)
        {
            throw new ConsumerException("Link is closed");
        }

        link.Accept(message);
        unsettledMessageCounter.Decrement();
    }

    public void Discard()
    {
        if (link.IsClosed)
        {
            throw new ConsumerException("Link is closed");
        }

        link.Reject(message);
        unsettledMessageCounter.Decrement();
    }

    public void Requeue()
    {
        if (!link.IsClosed)
        {
            throw new ConsumerException("Link is closed");
        }

        link.Release(message);
        unsettledMessageCounter.Decrement();
    }
}
