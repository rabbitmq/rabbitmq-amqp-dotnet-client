// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;

namespace RabbitMQ.AMQP.Client.Impl
{
    internal class AmqpManagementParameters
    {
        private readonly AmqpConnection _amqpConnection;

        private RecordingTopologyListener _topologyListener = null!;

        internal AmqpManagementParameters(AmqpConnection amqpConnection)
        {
            _amqpConnection = amqpConnection;
        }

        internal AmqpManagementParameters TopologyListener(RecordingTopologyListener topologyListener)
        {
            _topologyListener = topologyListener;
            return this;
        }

        internal AmqpConnection Connection => _amqpConnection;

        internal Amqp.Connection? NativeConnection => _amqpConnection.NativeConnection;

        internal bool IsNativeConnectionClosed
        {
            get
            {
                if (_amqpConnection.NativeConnection is null)
                {
                    // TODO create "internal bug" exception type?
                    throw new InvalidOperationException("NativeConnection is null, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
                }
                else
                {
                    return _amqpConnection.NativeConnection.IsClosed;
                }
            }
        }

        internal RecordingTopologyListener TopologyListener()
        {
            return _topologyListener;
        }
    }
}
