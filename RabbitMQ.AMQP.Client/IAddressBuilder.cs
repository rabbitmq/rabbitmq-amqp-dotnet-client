// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;

namespace RabbitMQ.AMQP.Client
{
    public class InvalidAddressException : Exception
    {
        public InvalidAddressException(string message) : base(message)
        {
        }
    }

    public interface IAddressBuilder<out T>
    {
        T Exchange(IExchangeSpecification exchangeSpec);
        T Exchange(string exchangeName);

        T Queue(IQueueSpecification queueSpec);
        T Queue(string queueName);

        T Key(string key);
    }

    public interface IMessageAddressBuilder : IAddressBuilder<IMessageAddressBuilder>
    {

        IMessage Build();
    }
}
