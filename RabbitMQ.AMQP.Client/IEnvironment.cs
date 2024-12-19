// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
    /// <summary>
    /// <para>
    ///   The <see cref="IEnvironment"/> is the main entry point to a node or a cluster of nodes.
    /// </para>
    /// <para>
    ///   The <see cref="CreateConnectionAsync()"/> method allows creating <see cref="IConnection"/> instances.
    ///   An application is expected to maintain a single <see cref="IEnvironment"/> instance and to close that instance
    ///   upon application exit.
    /// </para>
    /// <para>
    ///   <see cref="IEnvironment"/> instances are expected to be thread-safe.
    /// </para>
    /// </summary>
    public interface IEnvironment
    {
        /// <summary>
        /// Create a new <see cref="IConnection"/> with the given connection settings.
        /// </summary>
        /// <param name="connectionSettings"></param>
        /// <returns><see cref="Task{IConnection}"/> instance.</returns>
        public Task<IConnection> CreateConnectionAsync(ConnectionSettings connectionSettings);

        /// <summary>
        /// Create a new <see cref="IConnection"/> with the default connection settings.
        /// </summary>
        /// <returns><see cref="Task{IConnection}"/> instance.</returns>
        public Task<IConnection> CreateConnectionAsync();

        /// <summary>
        /// Close this environment and its resources.
        /// </summary>
        /// <returns><see cref="Task"/></returns>
        // TODO cancellation token
        Task CloseAsync();
    }
}
