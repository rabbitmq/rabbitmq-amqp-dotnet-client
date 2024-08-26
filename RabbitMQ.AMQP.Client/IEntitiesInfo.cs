// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.AMQP.Client;

public enum QueueType
{
    QUORUM,
    CLASSIC,
    STREAM
}

public interface IQueueInfo : IEntityInfo
{
    // TODO these should be properties, not methods
    string Name();

    bool Durable();

    bool AutoDelete();

    bool Exclusive();

    QueueType Type();

    // TODO IDictionary
    Dictionary<string, object> Arguments();

    string Leader();

    // TODO IEnumerable? ICollection?
    List<string> Replicas();

    ulong MessageCount();

    uint ConsumerCount();
}


public enum ExchangeType
{
    DIRECT,
    FANOUT,
    TOPIC,
    HEADERS
}
