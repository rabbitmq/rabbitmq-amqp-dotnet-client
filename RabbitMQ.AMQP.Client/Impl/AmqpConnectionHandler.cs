// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using Amqp;
using Amqp.Framing;
using Amqp.Handler;

namespace RabbitMQ.AMQP.Client.Impl
{
    /// <summary>
    /// Internal AMQP protocol handler used by <see cref="AmqpConnection"/>.
    /// <list type="bullet">
    ///   <item>
    ///     On <see cref="EventId.ConnectionLocalOpen"/>: injects custom properties (e.g. <c>connection_name</c>)
    ///     into the outgoing OPEN frame before it is sent.
    ///   </item>
    ///   <item>
    ///     On <see cref="EventId.ConnectionRemoteOpen"/>: extracts broker capabilities and version
    ///     from the incoming OPEN frame.
    ///   </item>
    ///   <item>
    ///     All events are forwarded to an optional internal handler
    ///     (e.g. the <see cref="ConnectionHandler"/> that dispatches <see cref="EventId.DeliveryStateChanged"/>
    ///     to individual <see cref="AmqpConsumer"/> instances) and to an optional user-supplied handler.
    ///   </item>
    /// </list>
    /// </summary>
    internal sealed class AmqpConnectionHandler : IHandler
    {
        private readonly Action<Open> _onLocalOpen;
        private readonly Action<Open> _onRemoteOpen;
        private readonly IHandler? _internalHandler;

        internal AmqpConnectionHandler(
            Action<Open> onLocalOpen,
            Action<Open> onRemoteOpen,
            IHandler? internalHandler = null)
        {
            _onLocalOpen = onLocalOpen;
            _onRemoteOpen = onRemoteOpen;
            _internalHandler = internalHandler;
        }

        public bool CanHandle(EventId id)
        {
            if (id is EventId.ConnectionLocalOpen or EventId.ConnectionRemoteOpen)
            {
                return true;
            }

            return _internalHandler != null && _internalHandler.CanHandle(id);
        }

        public void Handle(Event protocolEvent)
        {
            switch (protocolEvent.Id)
            {
                case EventId.ConnectionLocalOpen:
                    if (protocolEvent.Context is Open localOpen)
                    {
                        _onLocalOpen(localOpen);
                    }

                    break;

                case EventId.ConnectionRemoteOpen:
                    if (protocolEvent.Context is Open remoteOpen)
                    {
                        _onRemoteOpen(remoteOpen);
                    }
                    break;

            }

            if (_internalHandler != null && _internalHandler.CanHandle(protocolEvent.Id))
            {
                _internalHandler.Handle(protocolEvent);
            }

        }
    }
}
