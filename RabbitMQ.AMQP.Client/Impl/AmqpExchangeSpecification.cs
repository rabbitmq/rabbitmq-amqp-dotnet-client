// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl
{
    public class AmqpExchangeSpecification : IExchangeSpecification
    {
        private readonly AmqpManagement _management;
        private readonly ITopologyListener _topologyListener;

        public AmqpExchangeSpecification(AmqpManagement management)
        {
            _management = management;
            _topologyListener = ((IManagementTopology)_management).TopologyListener();
        }

        private string _name = "";
        private bool _autoDelete;
        private string _exchangeType = Client.ExchangeType.DIRECT.ToString();
        private readonly Map _arguments = new();

        public Task DeclareAsync()
        {
            if (string.IsNullOrEmpty(_name))
            {
                throw new ArgumentException("Exchange name must be set");
            }

            var kv = new Map
            {
                { "auto_delete", _autoDelete },
                { "arguments", _arguments },
                { "type", _exchangeType.ToLowerInvariant() },
                { "durable", true }
            };

            // TODO: encodePathSegment(queues)
            // Message request = await management.Request(kv, $"/{Consts.Exchanges}/{_name}",
            // for the moment we won't use the message response
            string path = $"/{Consts.Exchanges}/{Utils.EncodePathSegment(_name)}";
            string method = AmqpManagement.Put;
            int[] expectedResponseCodes = new int[] { AmqpManagement.Code204, AmqpManagement.Code201, AmqpManagement.Code409 };
            _topologyListener.ExchangeDeclared(this);
            return (Task)_management.RequestAsync(kv, path, method, expectedResponseCodes);
        }

        public Task DeleteAsync()
        {
            string path = $"/{Consts.Exchanges}/{Utils.EncodePathSegment(_name)}";
            string method = AmqpManagement.Delete;
            int[] expectedResponseCodes = new int[] { AmqpManagement.Code204 };
            _topologyListener.ExchangeDeleted(_name);
            return (Task)_management.RequestAsync(null, path, method, expectedResponseCodes);
        }

        public IExchangeSpecification Name(string name)
        {
            _name = name;
            return this;
        }

        public string ExchangeName => _name;

        public IExchangeSpecification AutoDelete(bool autoDelete)
        {
            _autoDelete = autoDelete;
            return this;
        }

        public bool IsAutoDelete => _autoDelete;

        public IExchangeSpecification Type(ExchangeType exchangeType)
        {
            _exchangeType = exchangeType.ToString();
            return this;
        }

        public IExchangeSpecification Type(string exchangeType)
        {
            _exchangeType = exchangeType;
            return this;
        }

        public string ExchangeType => _exchangeType;

        public IExchangeSpecification Argument(string key, object value)
        {
            _arguments[key] = value;
            return this;
        }

        public IExchangeSpecification Arguments(Dictionary<string, object> arguments)
        {
            foreach (string key in arguments.Keys)
            {
                object value = arguments[key];
                _arguments[key] = value;
            }
            return this;
        }

        public Dictionary<string, object> ExchangeArguments
        {
            get
            {
                var result = new Dictionary<string, object>();
                foreach (object key in _arguments.Keys)
                {
                    object value = _arguments[key];
                    result[key.ToString() ?? throw new InvalidOperationException()] = value;
                }
                return result;
            }
        }
    }
}
