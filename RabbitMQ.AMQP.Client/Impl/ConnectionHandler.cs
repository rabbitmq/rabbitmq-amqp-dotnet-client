// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using Amqp;
using Amqp.Handler;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

namespace RabbitMQ.AMQP.Client.Impl
{
    /// <summary>
    /// Routes amqpnetlite protocol events (specifically <see cref="EventId.DeliveryStateChanged"/>)
    /// to the appropriate <see cref="AmqpConsumer"/> based on the receiver link name.
    /// One instance is shared per <see cref="AmqpConnection"/>.
    /// </summary>
    internal sealed class ConnectionHandler : IHandler
    {
        private readonly ConcurrentDictionary<string, AmqpConsumer> _consumers = new();

        internal void RegisterConsumer(string linkName, AmqpConsumer consumer)
        {
            _consumers[linkName] = consumer;
        }

        internal void UnregisterConsumer(string linkName)
        {
            _consumers.TryRemove(linkName, out _);
        }

        public bool CanHandle(EventId id)
        {
            return id == EventId.DeliveryStateChanged;
        }

        public void Handle(Event protocolEvent)
        {
            if (protocolEvent.Id != EventId.DeliveryStateChanged)
            {
                return;
            }

            if (protocolEvent.Context is not IDelivery delivery)
            {
                return;
            }

            if (protocolEvent.Link is not ReceiverLink receiverLink)
            {
                return;
            }

            string linkName = receiverLink.Name;
            if (_consumers.TryGetValue(linkName, out AmqpConsumer? consumer))
            {
                try
                {
                    consumer.OnDeliveryStateChanged(receiverLink, delivery);
                }
                catch (Exception ex)
                {
                    Trace.WriteLine(TraceLevel.Error,
                        $"ConnectionHandler: error dispatching DeliveryStateChanged to {linkName}", ex);
                }
            }
        }
    }
}
