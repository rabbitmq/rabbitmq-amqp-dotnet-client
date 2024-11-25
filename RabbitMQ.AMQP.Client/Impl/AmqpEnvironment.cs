// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client.Impl
{
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

        // TODO to play nicely with IoC containers, we should not have static Create methods
        public static IEnvironment Create(ConnectionSettings connectionSettings, IMetricsReporter? metricsReporter = default)
        {
            return new AmqpEnvironment(connectionSettings, metricsReporter);
        }

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

        public Task<IConnection> CreateConnectionAsync()
        {
            if (ConnectionSettings is null)
            {
                throw new ConnectionException("Connection settings are not set");
            }

            return CreateConnectionAsync(ConnectionSettings);
        }

        public ReadOnlyCollection<IConnection> GetConnections() =>
            new(_connections.Values.ToList());

        public Task CloseAsync() => Task.WhenAll(_connections.Values.Select(c => c.CloseAsync()));
    }
}
