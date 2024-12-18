// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;

namespace RabbitMQ.AMQP.Client
{
    public interface IConnection : ILifeCycle
    {
        /// <summary>
        /// The <see cref="IManagement"/> instance for this connection.
        /// </summary>
        /// <returns><see cref="IManagement"/> instance for this connection.</returns>
        IManagement Management();

        /// <summary>
        /// Create an <see cref="IPublisherBuilder"/> instance for this connection.
        /// </summary>
        /// <returns><see cref="IPublisherBuilder"/> instance for this connection.</returns>
        IPublisherBuilder PublisherBuilder();

        /// <summary>
        /// Create an <see cref="IConsumerBuilder"/> instance for this connection.
        /// </summary>
        /// <returns><see cref="IConsumerBuilder"/> instance for this connection.</returns>
        IConsumerBuilder ConsumerBuilder();

        /// <summary>
        /// Create an <see cref="IRpcServerBuilder"/> instance for this connection.
        /// </summary>
        /// <returns><see cref="IRpcServerBuilder"/> instance for this connection.</returns>
        IRpcServerBuilder RpcServerBuilder();

        /// <summary>
        /// Create an <see cref="IRpcClientBuilder"/> instance for this connection.
        /// </summary>
        /// <returns><see cref="IRpcClientBuilder"/> instance for this connection.</returns>
        IRpcClientBuilder RpcClientBuilder();

        /// <summary>
        /// Get the properties for this connection.
        /// </summary>
        /// <returns><see cref="IReadOnlyDictionary{TKey, TValue}"/> of connection properties.</returns>
        public IReadOnlyDictionary<string, object> Properties { get; }

        /// <summary>
        /// Get the <see cref="IPublisher"/> instances associated with this connection.
        /// </summary>
        /// <returns><see cref="IEnumerable{T}"/> of <see cref="IPublisher"/> instances.</returns>
        public IEnumerable<IPublisher> Publishers { get; }

        /// <summary>
        /// Get the <see cref="IConsumer"/> instances associated with this connection.
        /// </summary>
        /// <returns><see cref="IEnumerable{T}"/> of <see cref="IConsumer"/> instances.</returns>
        public IEnumerable<IConsumer> Consumers { get; }

        /// <summary>
        /// Get or set the Connection ID. Used by <see cref="IEnvironment"/>
        /// </summary>
        public long Id { get; set; }
    }
}
