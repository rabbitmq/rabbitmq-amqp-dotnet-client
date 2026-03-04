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
        /// <summary>
        /// Queue where to apply the affinity. T
        /// The connection will try to find the node that has this queue for the given operation.
        /// </summary>
        /// <returns></returns>
        string Queue();

        /// <summary>
        /// Operation to apply the affinity to. It can be either publish or consume.
        /// </summary>
        /// <returns></returns>
        Operation Operation();

        /// <summary>
        /// Attempts to find the node that has the queue for the given operation
        /// before giving up and returning null.
        /// </summary>
        /// <returns></returns>
        uint Attempts();
    }

    /// <summary>
    /// DefaultAffinity is the default implementation of the IAffinity interface.
    /// </summary>
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

        public uint Attempts() => _tentatives;
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

            for (int i = 0; i < connectionSettings.Affinity.Attempts(); i++)
            {
                Trace.WriteLine(TraceLevel.Information,
                    $"Trying to find the node that has the queue {connectionSettings.Affinity.Queue()} for the operation {connectionSettings.Affinity.Operation()}, " +
                    $"attempt {i + 1}/{connectionSettings.Affinity.Attempts()}");
                // loop through the nodes and find the one that has the queue
                AmqpConnection connection = new(connectionSettings, metricsReporter);
                bool keepConnection = false;
                try
                {
                    await connection.OpenAsync()
                        .ConfigureAwait(false);

                    if (!TryExtractServerNameFromProperties(connection, out string? serverName))
                    {
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

                            keepConnection = true;
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
                                    keepConnection = true;
                                    return connection;
                                }

                                break;
                            case Operation.Consume:

                                if (queueInfo.Leader() != serverName!)
                                {
                                    Trace.WriteLine(TraceLevel.Verbose,
                                        $"The queue {connectionSettings.Affinity.Queue()} is found on the node {serverName} as a follower, " +
                                        $"the current connection will be returned.");

                                    keepConnection = true;
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
                        keepConnection = true;
                        return connection;
                    }
                }
                finally
                {
                    if (!keepConnection)
                    {
                        await connection.CloseAsync().ConfigureAwait(false);
                    }
                }
                await Task.Delay(300).ConfigureAwait(false);
            }

            return null;
        }
    }
}
