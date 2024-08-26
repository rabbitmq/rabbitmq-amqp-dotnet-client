// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace RabbitMQ.AMQP.Client.Impl
{
    internal interface IVisitor
    {
        Task VisitQueuesAsync(IEnumerable<QueueSpec> queueSpec);
        Task VisitExchangesAsync(IEnumerable<ExchangeSpec> exchangeSpec);
        Task VisitBindingsAsync(IEnumerable<BindingSpec> bindingSpec);
    }

    /// <summary>
    /// RecordingTopologyListener is a concrete implementation of <see cref="ITopologyListener"/>
    ///  It is used to record the topology of the entities declared in the AMQP server ( like queues, exchanges, etc)
    /// It is used to recover the topology of the server after a connection is established in case of a reconnection
    /// Each time am entity is declared or deleted, the listener will record the event
    /// </summary>
    public class RecordingTopologyListener : ITopologyListener
    {
        private readonly ConcurrentDictionary<string, QueueSpec> _queueSpecifications = new();

        private readonly ConcurrentDictionary<string, ExchangeSpec> _exchangeSpecifications = new();

        private readonly ConcurrentDictionary<string, BindingSpec> _bindingSpecifications = new();

        private void RemoveBindingsSpecificationFromQueue(string queueName)
        {
            foreach (var binding in _bindingSpecifications.Values)
            {
                if (binding.DestinationQueue == queueName)
                {
                    _bindingSpecifications.TryRemove(binding.Path, out _);
                }
            }
        }

        private void RemoveBindingsSpecificationFromExchange(string exchangeName)
        {
            foreach (var binding in _bindingSpecifications.Values)
            {
                if (binding.SourceExchange == exchangeName
                    || binding.DestinationExchange == exchangeName)
                {
                    _bindingSpecifications.TryRemove(binding.Path, out _);
                }
            }
        }

        public void QueueDeclared(IQueueSpecification specification)
        {
            _queueSpecifications.TryAdd(specification.Name(), new QueueSpec(specification));
        }

        public void QueueDeleted(string name)
        {
            _queueSpecifications.TryRemove(name, out _);
            RemoveBindingsSpecificationFromQueue(name);
        }

        public void ExchangeDeclared(IExchangeSpecification specification)
        {
            _exchangeSpecifications.TryAdd(specification.Name(), new ExchangeSpec(specification));
        }

        public void ExchangeDeleted(string name)
        {
            _exchangeSpecifications.TryRemove(name, out _);
            RemoveBindingsSpecificationFromExchange(name);
        }

        public void BindingDeclared(IBindingSpecification specification)
        {
            _bindingSpecifications.TryAdd(specification.Path(), new BindingSpec(specification));
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


        internal async Task Accept(IVisitor visitor)
        {
            await visitor.VisitQueuesAsync(_queueSpecifications.Values).ConfigureAwait(false);

            await visitor.VisitExchangesAsync(_exchangeSpecifications.Values).ConfigureAwait(false);

            await visitor.VisitBindingsAsync(_bindingSpecifications.Values).ConfigureAwait(false);
        }
    }

    internal class QueueSpec
    {
        private readonly IQueueSpecification _queueSpecification;

        internal QueueSpec(IQueueSpecification queueSpecification)
        {
            _queueSpecification = queueSpecification;
        }

        internal string Name
        {
            get
            {
                return _queueSpecification.Name();
            }
        }

        internal bool Exclusive
        {
            get
            {
                return _queueSpecification.Exclusive();
            }
        }

        internal bool AutoDelete
        {
            get
            {
                return _queueSpecification.AutoDelete();
            }
        }

        internal Dictionary<object, object> Arguments
        {
            get
            {
                return _queueSpecification.Arguments();
            }
        }
    }

    internal class ExchangeSpec
    {
        private readonly IExchangeSpecification _exchangeSpecification;

        internal ExchangeSpec(IExchangeSpecification exchangeSpecification)
        {
            _exchangeSpecification = exchangeSpecification;
        }

        internal string Name
        {
            get
            {
                return _exchangeSpecification.Name();
            }
        }

        internal ExchangeType Type
        {
            get
            {
                return _exchangeSpecification.Type();
            }
        }

        internal bool AutoDelete
        {
            get
            {
                return _exchangeSpecification.AutoDelete();
            }
        }

        internal Dictionary<string, object> Arguments
        {
            get
            {
                return _exchangeSpecification.Arguments();
            }
        }
    }

    internal class BindingSpec
    {
        private readonly IBindingSpecification _bindingSpecification;

        internal BindingSpec(IBindingSpecification bindingSpecification)
        {
            _bindingSpecification = bindingSpecification;
        }

        internal string SourceExchange
        {
            get
            {
                return _bindingSpecification.SourceExchangeName();
            }
        }

        internal string DestinationExchange
        {
            get
            {
                return _bindingSpecification.DestinationExchangeName();
            }
        }

        internal string DestinationQueue
        {
            get
            {
                return _bindingSpecification.DestinationQueueName();
            }
        }

        internal string Key
        {
            get
            {
                return _bindingSpecification.Key();
            }
        }

        internal Dictionary<string, object> Arguments
        {
            get
            {
                return _bindingSpecification.Arguments();
            }
        }

        internal string Path
        {
            get
            {
                return _bindingSpecification.Path();
            }
        }
    }
}
