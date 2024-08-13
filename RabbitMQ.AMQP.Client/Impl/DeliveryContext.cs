// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class DeliveryContext(IReceiverLink link, Message message, UnsettledMessageCounter unsettledMessageCounter) : IContext
{
    public Task AcceptAsync()
    {
        if (link.IsClosed)
        {
            throw new ConsumerException("Link is closed");
        }

        Task acceptTask = Task.Run(() =>
        {
            link.Accept(message);
            unsettledMessageCounter.Decrement();
            message.Dispose();
        });

        return acceptTask;
    }

    public Task DiscardAsync()
    {
        if (link.IsClosed)
        {
            throw new ConsumerException("Link is closed");
        }

        Task rejectTask = Task.Run(() =>
        {
            link.Reject(message);
            unsettledMessageCounter.Decrement();
            message.Dispose();
        });

        return rejectTask;
    }

    public Task RequeueAsync()
    {
        if (link.IsClosed)
        {
            throw new ConsumerException("Link is closed");
        }

        Task requeueTask = Task.Run(() =>
        {
            link.Release(message);
            unsettledMessageCounter.Decrement();
            message.Dispose();
        });

        return requeueTask;
    }
}
