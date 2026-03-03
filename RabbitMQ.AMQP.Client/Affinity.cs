// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using RabbitMQ.AMQP.Client.Impl;

namespace RabbitMQ.AMQP.Client
{
    public enum Operation
    {
        Publish,
        // TODO: Consume
    }

    public interface IAffinity
    {
        string Queue();

        Operation Operation();
    }

    public class DefaultAffinity : IAffinity
    {
        private readonly string _queue;
        private readonly Operation _operation;

        public DefaultAffinity(string queue, Operation operation)
        {
            _queue = queue;
            _operation = operation;
        }

        public string Queue()
        {
            return _queue;
        }

        public Operation Operation()
        {
            return _operation;
        }
    }

    public static class AffinityUtils
    {
        private static bool TryExtractServerNameFromProperties(IConnection connection, out string? name)
        {
            if (connection.Properties.TryGetValue("node", out object? nodeName))
            {
                name = (string)nodeName;
                return true;
            }

            if (connection.Properties.TryGetValue("server", out object? serverName))
            {
                name = (string)serverName;
                return true;
            }

            name = null;
            return false;
        }

        public static async Task<IConnection?> TryToFindUriNode(ConnectionSettings connectionSettings,
            IMetricsReporter? metricsReporter)
        {
            for (int i = 0; i < 10; i++)
            {
                if (connectionSettings.Affinity == null)
                {
                    // raise an exception or return a default value if the affinity is not set in the connection settings
                    throw new NullReferenceException("Affinity is not set in the connection settings");
                }

                // loop through the nodes and find the one that has the queue
                AmqpConnection connection = new(connectionSettings, metricsReporter);
                await connection.OpenAsync()
                    .ConfigureAwait(false);

                if (!TryExtractServerNameFromProperties(connection, out string? serverName))
                {
                    // raise an exception or return a default value if the server name cannot be extracted from the connection properties
                    throw new KeyNotFoundException("Cannot extract server name from connection properties");
                }

                try
                {
                    var s = await connection.Management().GetQueueInfoAsync(connectionSettings.Affinity.Queue())
                        .ConfigureAwait(false);
                    if (s.Leader() == serverName!)
                    {
                        return connection;
                    }
                }
                catch (ResourceNotFoundException)
                {
                    // resource not found, which means the queue does not exist. 
                    // we can go ahead and return null. the caller can decide what to do.
                    // affinity is a best effort feature, so if we cannot find the node with the queue,
                    // we can just return null and let the caller handle it. it can use the default settings.
                    return null;
                }

                await connection.CloseAsync().ConfigureAwait(false);
                await Task.Delay(300).ConfigureAwait(false);
            }

            return null;
        }
    }
}
