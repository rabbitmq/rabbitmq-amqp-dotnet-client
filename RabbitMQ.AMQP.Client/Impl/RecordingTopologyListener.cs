// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Concurrent;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client.Impl
{
    /// <summary>
    /// RecordingTopologyListener is a concrete implementation of <see cref="ITopologyListener"/>
    ///  It is used to record the topology of the entities declared in the AMQP server ( like queues, exchanges, etc)
    /// It is used to recover the topology of the server after a connection is established in case of a reconnection
    /// Each time am entity is declared or deleted, the listener will record the event
    /// </summary>
    internal class RecordingTopologyListener : ITopologyListener
    {
        private readonly ConcurrentDictionary<string, QueueSpec> _queueSpecifications = new();
        private readonly ConcurrentDictionary<string, ExchangeSpec> _exchangeSpecifications = new();
        private readonly ConcurrentDictionary<string, BindingSpec> _bindingSpecifications = new();

        public void QueueDeclared(IQueueSpecification specification)
        {
            _queueSpecifications.TryAdd(specification.QueueName, new QueueSpec(specification));
        }

        public void QueueDeleted(string name)
        {
            _queueSpecifications.TryRemove(name, out _);
            RemoveBindingsSpecificationFromQueue(name);
        }

        public void ExchangeDeclared(IExchangeSpecification specification)
        {
            _exchangeSpecifications.TryAdd(specification.ExchangeName, new ExchangeSpec(specification));
        }

        public void ExchangeDeleted(string name)
        {
            _exchangeSpecifications.TryRemove(name, out _);
            RemoveBindingsSpecificationFromExchange(name);
        }

        public void BindingDeclared(IBindingSpecification specification)
        {
            _bindingSpecifications.TryAdd(specification.BindingPath, new BindingSpec(specification));
        }

        public void BindingDeleted(string key)
        {
            _bindingSpecifications.TryRemove(key, out _);
        }

        public void Clear()
        {
            _queueSpecifications.Clear();
            _exchangeSpecifications.Clear();
            _bindingSpecifications.Clear();
        }

        public int QueueCount()
        {
            return _queueSpecifications.Count;
        }

        public int ExchangeCount()
        {
            return _exchangeSpecifications.Count;
        }

        public int BindingCount() => _bindingSpecifications.Count;

        private void RemoveBindingsSpecificationFromQueue(string queueName)
        {
            foreach (BindingSpec binding in _bindingSpecifications.Values)
            {
                if (binding.DestinationQueueName == queueName)
                {
                    _bindingSpecifications.TryRemove(binding.BindingPath, out _);
                }
            }
        }

        private void RemoveBindingsSpecificationFromExchange(string exchangeName)
        {
            foreach (BindingSpec binding in _bindingSpecifications.Values)
            {
                if (binding.SourceExchangeName == exchangeName
                    || binding.DestinationExchangeName == exchangeName)
                {
                    _bindingSpecifications.TryRemove(binding.BindingPath, out _);
                }
            }
        }

        internal async Task Accept(IVisitor visitor)
        {
            await visitor.VisitQueuesAsync(_queueSpecifications.Values)
                .ConfigureAwait(false);

            await visitor.VisitExchangesAsync(_exchangeSpecifications.Values)
                .ConfigureAwait(false);

            await visitor.VisitBindingsAsync(_bindingSpecifications.Values)
                .ConfigureAwait(false);
        }
    }
}
