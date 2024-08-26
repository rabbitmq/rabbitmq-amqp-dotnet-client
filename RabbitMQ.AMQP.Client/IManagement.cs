// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.AMQP.Client;

public class ModelException(string message) : Exception(message);

public class PreconditionFailedException(string message) : Exception(message);

public interface IManagement : ILifeCycle
{
    IQueueSpecification Queue();
    IQueueSpecification Queue(string name);

    Task<IQueueInfo> GetQueueInfoAsync(IQueueSpecification queueSpec,
        CancellationToken cancellationToken = default);
    Task<IQueueInfo> GetQueueInfoAsync(string queueName,
        CancellationToken cancellationToken = default);

    IExchangeSpecification Exchange();
    IExchangeSpecification Exchange(string name);

    IBindingSpecification Binding();
}

internal interface IManagementTopology
{
    ITopologyListener TopologyListener();
}
