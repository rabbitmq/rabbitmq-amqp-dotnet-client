// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
    public class ModelException : Exception
    {
        public ModelException(string message) : base(message)
        {
        }
    }

    public class PreconditionFailedException : Exception
    {
        public PreconditionFailedException(string message) : base(message)
        {
        }
    }

    public class BadRequestException : Exception
    {
        public BadRequestException(string message) : base(message)
        {
        }
    }

    /// <summary>
    /// IManagement interface and is responsible for managing the AMQP resources.
    /// RabbitMQ uses AMQP end point: "/management" to manage the resources like queues, exchanges, and bindings.
    /// The management endpoint works like an HTTP RPC endpoint where the client sends a request to the server.
    /// </summary>
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
}
