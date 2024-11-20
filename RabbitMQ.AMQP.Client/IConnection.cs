// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;

namespace RabbitMQ.AMQP.Client
{
    public class ConnectionException : Exception
    {
        public ConnectionException(string message) : base(message)
        {
        }

        public ConnectionException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }

    public interface IConnection : ILifeCycle
    {
        IManagement Management();

        IPublisherBuilder PublisherBuilder();

        IConsumerBuilder ConsumerBuilder();

        IRpcServerBuilder RpcServerBuilder();

        IRpcClientBuilder RpcClientBuilder();

        public IReadOnlyDictionary<string, object> Properties { get; }

        public IEnumerable<IPublisher> Publishers { get; }

        public IEnumerable<IConsumer> Consumers { get; }

        public long Id { get; set; }
    }
}
