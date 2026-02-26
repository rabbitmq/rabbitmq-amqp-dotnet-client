// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client.Impl
{
    /// <summary>
    /// <para>
    ///   <see cref="AmqpEnvironment"/> is the implementation of <see cref="IEnvironment"/>.
    /// </para>
    /// <para>
    ///   The <see cref="CreateConnectionAsync()"/> method allows creating <see cref="IConnection"/> instances.
    /// </para>
    /// </summary>
    public class AmqpEnvironment : IEnvironment
    {
        private ConnectionSettings ConnectionSettings { get; }
        private long _sequentialId = 0;
        private readonly ConcurrentDictionary<long, IConnection> _connections = new();
        private readonly IMetricsReporter? _metricsReporter;

        private AmqpEnvironment(ConnectionSettings connectionSettings, IMetricsReporter? metricsReporter = default)
        {
            ConnectionSettings = connectionSettings;
            _metricsReporter = metricsReporter;
        }

        /// <summary>
        /// Create a new <see cref="IEnvironment"/> instance, using the provided <see cref="ConnectionSettings"/>
        /// and optional <see cref="IMetricsReporter"/>
        /// </summary>
        /// <param name="connectionSettings"></param>
        /// <param name="metricsReporter"></param>
        /// <returns><see cref="IEnvironment"/> instance.</returns>
        public static IEnvironment Create(ConnectionSettings connectionSettings, IMetricsReporter? metricsReporter = default)
        {
            // TODO to play nicely with IoC containers, we should not have static Create methods
            return new AmqpEnvironment(connectionSettings, metricsReporter);
        }

        /// <summary>
        /// Create a new <see cref="IConnection"/> instance, using the provided <see cref="ConnectionSettings"/>.
        /// </summary>
        /// <param name="connectionSettings"></param>
        /// <returns><see cref="Task{IConnection}"/> instance.</returns>
        public async Task<IConnection> CreateConnectionAsync(ConnectionSettings connectionSettings)
        {
            IConnection c = await AmqpConnection.CreateAsync(connectionSettings, _metricsReporter).ConfigureAwait(false);
            c.Id = Interlocked.Increment(ref _sequentialId);
            _connections.TryAdd(c.Id, c);
            c.ChangeState += (sender, previousState, currentState, failureCause) =>
            {
                if (currentState != State.Closed)
                {
                    return;
                }

                if (sender is IConnection connection)
                {
                    _connections.TryRemove(connection.Id, out _);
                }
            };
            return c;
        }

        /// <summary>
        /// Create a new <see cref="IConnection"/> instance, using the <see cref="IEnvironment"/> <see cref="ConnectionSettings"/>.
        /// </summary>
        /// <returns><see cref="Task{IConnection}"/> instance.</returns>
        public Task<IConnection> CreateConnectionAsync()
        {
            if (ConnectionSettings is null)
            {
                throw new ConnectionException("Connection settings are not set");
            }

            return CreateConnectionAsync(ConnectionSettings);
        }

        /// <summary>
        /// Close this environment and its resources.
        /// </summary>
        /// <returns><see cref="Task"/></returns>
        // TODO cancellation token
        public Task CloseAsync()
        {
            return Task.WhenAll(_connections.Values.Select(c => c.CloseAsync()));
        }

        internal IList<IConnection> Connections => _connections.Values.ToList();
    }
}
