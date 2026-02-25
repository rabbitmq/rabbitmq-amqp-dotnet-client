// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
    public partial interface IConnectionBuilder
    {
        /// <summary>
        /// Set the <see cref="IMetricsReporter"/> for this connection.
        /// </summary>
        /// <param name="metricsReporter">The <see cref="IMetricsReporter"/> instance to use for this connection.</param>
        /// <returns>The current <see cref="IConnectionBuilder"/> instance for chaining.</returns>
        public IConnectionBuilder MetricsReporter(IMetricsReporter? metricsReporter);

        /// <summary>
        /// Set the <see cref="ConnectionSettings"/> for this connection.
        /// </summary>
        public IConnectionBuilder ConnectionSettings(ConnectionSettings connectionSettings);

        /// <summary>
        /// Create the connection asynchronously.
        /// </summary>
        /// <param name="cancellationToken">Cancellation token to cancel the build operation.</param>
        /// <returns>A task that represents the asynchronous build operation. The task result contains the built <see cref="IConnection"/> instance.</returns>
        public Task<IConnection> CreateConnectionAsync(CancellationToken cancellationToken = default);

    }

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
        /// Create an <see cref="IResponderBuilder"/> instance for this connection.
        /// </summary>
        /// <returns><see cref="IResponderBuilder"/> instance for this connection.</returns>
        IResponderBuilder ResponderBuilder();

        /// <summary>
        /// Create an <see cref="IRequesterBuilder"/> instance for this connection.
        /// </summary>
        /// <returns><see cref="IRequesterBuilder"/> instance for this connection.</returns>
        IRequesterBuilder RequesterBuilder();

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

        public Task RefreshTokenAsync(string token);
    }
}
