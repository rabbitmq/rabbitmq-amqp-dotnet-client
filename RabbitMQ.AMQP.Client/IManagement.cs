// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
    /// <summary>
    /// The <see cref="IManagement"/> interface is used to manage the
    /// <see href="https://www.rabbitmq.com/tutorials/amqp-concepts">AMQP 0.9.1 model</see> 
    /// topology (exchanges, queues, and bindings).
    /// </summary>
    public interface IManagement : ILifeCycle
    {
        /// <summary>
        /// Create an <see cref="IQueueSpecification"/>, with an auto-generated name.
        /// </summary>
        /// <returns>A builder for <see cref="IQueueSpecification"/></returns>
        IQueueSpecification Queue();

        /// <summary>
        /// Create an <see cref="IQueueSpecification"/>, with the given name.
        /// </summary>
        /// <returns>A builder for <see cref="IQueueSpecification"/></returns>
        IQueueSpecification Queue(string name);

        /// <summary>
        /// Get the <see cref="IQueueInfo"/> for the given queue specification.
        /// </summary>
        /// <param name="queueSpec">The <see cref="IQueueSpecification"/></param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/></param>
        /// <returns>The <see cref="IQueueInfo"/> for the given spec.</returns>
        Task<IQueueInfo> GetQueueInfoAsync(IQueueSpecification queueSpec,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Get the <see cref="IQueueInfo"/> for the given queue name.
        /// </summary>
        /// <param name="queueName">The queue name</param>
        /// <param name="cancellationToken">The <see cref="CancellationToken"/></param>
        /// <returns>The <see cref="IQueueInfo"/> for the given spec.</returns>
        Task<IQueueInfo> GetQueueInfoAsync(string queueName,
            CancellationToken cancellationToken = default);

        /// <summary>
        /// Create an <see cref="IExchangeSpecification"/>, with an auto-generated name.
        /// </summary>
        /// <returns>A builder for <see cref="IExchangeSpecification"/></returns>
        IExchangeSpecification Exchange();

        /// <summary>
        /// Create an <see cref="IExchangeSpecification"/>, with the given name.
        /// </summary>
        /// <returns>A builder for <see cref="IExchangeSpecification"/></returns>
        IExchangeSpecification Exchange(string name);

        /// <summary>
        /// Create an <see cref="IBindingSpecification"/>.
        /// </summary>
        /// <returns>A builder for <see cref="IBindingSpecification"/></returns>
        IBindingSpecification Binding();

    }

    internal interface IManagementTopology
    {
        ITopologyListener TopologyListener();
    }
}
