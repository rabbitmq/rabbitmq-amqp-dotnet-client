// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.ObjectModel;

namespace RabbitMQ.AMQP.Client;

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

    public ReadOnlyCollection<IPublisher> GetPublishers();

    public ReadOnlyCollection<IConsumer> GetConsumers();

    public long Id { get; set; }
}
