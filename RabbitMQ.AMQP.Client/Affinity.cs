// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amqp;
using RabbitMQ.AMQP.Client.Impl;

namespace RabbitMQ.AMQP.Client
{
    public enum Operation
    {
        Publish,
        Consume
    }

    /// <summary>
    /// IAffinity the interface to define node affinity for a connection.
    /// It is used to find the node that has the queue for the given operation.
    /// </summary>
    public interface IAffinity
    {
        string Queue();

        Operation Operation();

        uint Tentatives();
    }

    public class DefaultAffinity : IAffinity
    {
        private readonly string _queue;
        private readonly Operation _operation;
        private readonly uint _tentatives;

        public DefaultAffinity(string queue, Operation operation, uint tentatives = 10)
        {
            _queue = queue;
            _operation = operation;
            _tentatives = tentatives;
        }

        public string Queue()
        {
            return _queue;
        }

        public Operation Operation()
        {
            return _operation;
        }

        public uint Tentatives() => _tentatives;
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
            if (connectionSettings.Affinity == null)
            {
                Trace.WriteLine(TraceLevel.Warning, "Affinity is not specified in the connection settings, " +
                                                    "the affinity connection will not be created.");
                return null;
            }

            for (int i = 0; i < connectionSettings.Affinity.Tentatives(); i++)
            {
                Trace.WriteLine(TraceLevel.Information,
                    $"Trying to find the node that has the queue {connectionSettings.Affinity.Queue()} for the operation {connectionSettings.Affinity.Operation()}, " +
                    $"attempt {i + 1}/{connectionSettings.Affinity.Tentatives()}");
                // loop through the nodes and find the one that has the queue
                AmqpConnection connection = new(connectionSettings, metricsReporter);
                await connection.OpenAsync()
                    .ConfigureAwait(false);

                if (!TryExtractServerNameFromProperties(connection, out string? serverName))
                {
                    await connection.CloseAsync().ConfigureAwait(false);
                    Trace.WriteLine(TraceLevel.Warning, $"can't extract server name from connection properties, " +
                                                        $"the connection will be closed. properties: {string.Join(", ", connection.Properties)}");

                    return null;
                }

                try
                {
                    var queueInfo = await connection.Management().GetQueueInfoAsync(connectionSettings.Affinity.Queue())
                        .ConfigureAwait(false);

                    // there are no replicas for the queue,
                    // which means the queue is only on one node,
                    // we can return the current connection directly.
                    if (queueInfo.Members().Count == 0)
                    {
                        Trace.WriteLine(TraceLevel.Verbose,
                            $"The queue {connectionSettings.Affinity.Queue()} is found on the node {serverName}, " +
                            $"no members found, the current connection will be returned.");

                        return connection;
                    }

                    switch (connectionSettings.Affinity.Operation())
                    {
                        case Operation.Publish:
                            if (queueInfo.Leader() == serverName!)
                            {
                                Trace.WriteLine(TraceLevel.Verbose,
                                    $"The queue {connectionSettings.Affinity.Queue()} is found on the node {serverName}, " +
                                    $"the current connection will be returned.");
                                return connection;
                            }

                            break;
                        case Operation.Consume:

                            if (queueInfo.Leader() != serverName!)
                            {
                                Trace.WriteLine(TraceLevel.Verbose,
                                    $"The queue {connectionSettings.Affinity.Queue()} is found on the node {serverName} as a follower, " +
                                    $"the current connection will be returned.");

                                return connection;
                            }

                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }
                }
                catch (ResourceNotFoundException)
                {
                    Trace.WriteLine(TraceLevel.Verbose,
                        $"The queue {connectionSettings.Affinity.Queue()} is not found on the node {serverName}, " +
                        $"the current connection will be returned. This might be because the queue is not created yet");

                    // resource not found, which means the queue does not exist. 
                    // we can go ahead and return the current connection.
                    return connection;
                }

                await connection.CloseAsync().ConfigureAwait(false);
                await Task.Delay(300).ConfigureAwait(false);
            }

            return null;
        }
    }
}
