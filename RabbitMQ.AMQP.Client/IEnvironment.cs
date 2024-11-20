// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.ObjectModel;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
    /// <summary>
    /// Interface to create IConnections and manage them.
    /// </summary>
    public interface IEnvironment
    {
        /// <summary>
        /// Create a new connection with the given connection settings.
        /// </summary>
        /// <param name="connectionSettings"></param>
        /// <returns>IConnection</returns>
        public Task<IConnection> CreateConnectionAsync(IConnectionSettings connectionSettings);

        /// <summary>
        /// Create a new connection with the default connection settings.
        /// </summary>
        /// <returns>IConnection</returns>
        public Task<IConnection> CreateConnectionAsync();

        /// <summary>
        /// Get all connections.
        /// </summary>
        public ReadOnlyCollection<IConnection> GetConnections();

        /// <summary>
        /// Close all connections.
        /// </summary>
        /// <returns></returns>
        Task CloseAsync();
    }
}
