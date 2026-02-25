// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client
{
    /// <summary>
    /// <para>
    ///   The <see cref="IEnvironment"/> is the main entry point to a node or a cluster of nodes.
    ///   Use <see cref="ConnectionBuilder"/> to create <see cref="IConnection"/> instances.
    /// </para>
    /// <para>
    ///   <see cref="IEnvironment"/> instances are expected to be thread-safe.
    /// </para>
    /// </summary>
    public interface IEnvironment
    {
        /// <summary>
        /// Returns a builder for creating <see cref="IConnection"/> instances with optional settings and cancellation support.
        /// </summary>
        public IConnectionBuilder ConnectionBuilder();

        /// <summary>
        /// Close this environment and its resources.
        /// </summary>
        /// <returns><see cref="Task"/></returns>
        // TODO cancellation token
        Task CloseAsync();

        // Deprecated: use ConnectionBuilder().CreateConnectionAsync() instead, which allows passing a cancellation token and connection settings.
        [Obsolete("Use ConnectionBuilder() instead, which allows passing a cancellation token and connection settings.")]
        Task<IConnection> CreateConnectionAsync();

        // Deprecated: use ConnectionBuilder().CreateConnectionAsync() instead, which allows passing a cancellation token and connection settings.
        [Obsolete("Use ConnectionBuilder() instead, which allows passing a cancellation token and connection settings.")]
        Task<IConnection> CreateConnectionAsync(ConnectionSettings connectionSettings);
    }
}
