// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;

namespace RabbitMQ.AMQP.Client.Impl
{
    public abstract class DefaultAddressBuilder<T> : IAddressBuilder<T>
    {
        private string? _exchange = null;
        private string? _queue = null;
        private string? _key = null;
        protected T? _owner = default;

        public T Exchange(IExchangeSpecification exchangeSpec)
        {
            return Exchange(exchangeSpec.ExchangeName);
        }

        public T Exchange(string? exchangeName)
        {
            _exchange = exchangeName;
            if (_owner == null)
            {
                throw new InvalidOperationException("Owner is null");
            }

            return _owner;
        }

        public T Queue(IQueueSpecification queueSpec) => Queue(queueSpec.QueueName);

        public T Queue(string? queueName)
        {
            _queue = queueName;
            if (_owner == null)
            {
                throw new InvalidOperationException("Owner is null");
            }

            return _owner;
        }

        public T Key(string? key)
        {
            _key = key;
            if (_owner == null)
            {
                throw new InvalidOperationException("Owner is null");
            }

            return _owner;
        }

        public string Address()
        {
            if (_exchange == null && _queue == null)
            {
                throw new InvalidAddressException("Exchange or Queue must be set");
            }

            if (_exchange != null && _queue != null)
            {
                throw new InvalidAddressException("Exchange and Queue cannot be set together");
            }

            if (_exchange != null)
            {
                if (string.IsNullOrEmpty(_exchange))
                {
                    throw new InvalidAddressException("Exchange must be set");
                }

                if (_key != null && false == string.IsNullOrEmpty(_key))
                {
                    return $"/{Consts.Exchanges}/{Utils.EncodePathSegment(_exchange)}/{Utils.EncodePathSegment(_key)}";
                }

                return $"/{Consts.Exchanges}/{Utils.EncodePathSegment(_exchange)}";
            }

            if (_queue == null)
            {
                return "";
            }

            if (string.IsNullOrEmpty(_queue))
            {
                throw new InvalidAddressException("Queue must be set");
            }

            return $"/{Consts.Queues}/{Utils.EncodePathSegment(_queue)}";
        }
    }

    public class AddressBuilder : DefaultAddressBuilder<AddressBuilder>
    {
        public AddressBuilder()
        {
            _owner = this;
        }
    }

    public static class AddressBuilderHelper
    {
        public static AddressBuilder AddressBuilder() => new();
    }

    public class MessageAddressBuilder : DefaultAddressBuilder<IMessageAddressBuilder>, IMessageAddressBuilder
    {
        private readonly IMessage _message;

        public MessageAddressBuilder(IMessage message)
        {
            _message = message;
            _owner = this;
        }

        public IMessage Build()
        {
            _message.To(Address());
            return _message;
        }
    }

    public class RpcClientAddressBuilder : DefaultAddressBuilder<IRpcClientAddressBuilder>, IRpcClientAddressBuilder
    {
        readonly AmqpRpcClientBuilder _builder;
        public RpcClientAddressBuilder(AmqpRpcClientBuilder builder)
        {
            _builder = builder;
            _owner = this;
        }

        public IRpcClientBuilder RpcClient()
        {
            return _builder;
        }
    }
}
