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
    internal class RecordingTopologyListener : ITopologyListener
    {
        private readonly ConcurrentDictionary<string, QueueSpec> _queueSpecifications = new();

        private readonly ConcurrentDictionary<string, ExchangeSpec> _exchangeSpecifications = new();

        private readonly ConcurrentDictionary<string, BindingSpec> _bindingSpecifications = new();

        private void RemoveBindingsSpecificationFromQueue(string queueName)
        {
            foreach (var binding in _bindingSpecifications.Values)
            {
                if (binding.DestinationQueueName == queueName)
                {
                    _bindingSpecifications.TryRemove(binding.BindingPath, out _);
                }
            }
        }

        private void RemoveBindingsSpecificationFromExchange(string exchangeName)
        {
            foreach (var binding in _bindingSpecifications.Values)
            {
                if (binding.SourceExchangeName == exchangeName
                    || binding.DestinationExchangeName == exchangeName)
                {
                    _bindingSpecifications.TryRemove(binding.BindingPath, out _);
                }
            }
        }

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

        internal string QueueName
        {
            get
            {
                return _queueSpecification.QueueName;
            }
        }

        internal bool IsExclusive
        {
            get
            {
                return _queueSpecification.IsExclusive;
            }
        }

        internal bool IsAutoDelete
        {
            get
            {
                return _queueSpecification.IsAutoDelete;
            }
        }

        internal Dictionary<object, object> QueueArguments
        {
            get
            {
                return _queueSpecification.QueueArguments;
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

        internal string ExchangeName
        {
            get
            {
                return _exchangeSpecification.ExchangeName;
            }
        }

        internal ExchangeType ExchangeType
        {
            get
            {
                return _exchangeSpecification.ExchangeType;
            }
        }

        internal bool IsAutoDelete
        {
            get
            {
                return _exchangeSpecification.IsAutoDelete;
            }
        }

        internal Dictionary<string, object> ExchangeArguments
        {
            get
            {
                return _exchangeSpecification.ExchangeArguments;
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

        internal string SourceExchangeName
        {
            get
            {
                return _bindingSpecification.SourceExchangeName;
            }
        }

        internal string DestinationExchangeName
        {
            get
            {
                return _bindingSpecification.DestinationExchangeName;
            }
        }

        internal string DestinationQueueName
        {
            get
            {
                return _bindingSpecification.DestinationQueueName;
            }
        }

        internal string BindingKey
        {
            get
            {
                return _bindingSpecification.BindingKey;
            }
        }

        internal Dictionary<string, object> BindingArguments
        {
            get
            {
                return _bindingSpecification.BindingArguments;
            }
        }

        internal string BindingPath
        {
            get
            {
                return _bindingSpecification.BindingPath;
            }
        }
    }
}
