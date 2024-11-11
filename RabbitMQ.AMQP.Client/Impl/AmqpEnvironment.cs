// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client.Impl
{
    public class AmqpEnvironment : IEnvironment
    {
        private IConnectionSettings ConnectionSettings { get; }
        private long _sequentialId = 0;
        private readonly ConcurrentDictionary<long, IConnection> _connections = new();
        private readonly IMetricsReporter? _metricsReporter;

        private AmqpEnvironment(IConnectionSettings connectionSettings, IMetricsReporter? metricsReporter)
        {
            ConnectionSettings = connectionSettings;
            _metricsReporter = metricsReporter;
        }

        public static Task<IEnvironment> CreateAsync(IConnectionSettings connectionSettings,
            IMetricsReporter? metricsReporter = default)
        {
            return Task.FromResult<IEnvironment>(new AmqpEnvironment(connectionSettings,
                metricsReporter ?? new NoOpMetricsReporter()));
        }

        public async Task<IConnection> CreateConnectionAsync(IConnectionSettings connectionSettings)
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
