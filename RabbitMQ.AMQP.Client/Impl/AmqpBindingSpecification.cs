// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Generic;
using System.Threading.Tasks;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl
{
    public abstract class BindingSpecification
    {
        protected string _source = "";
        protected string _destination = "";
        protected string _routingKey = "";
        protected bool _toQueue = true;
        protected Dictionary<string, object> _arguments = new();

        protected Map ArgsToMap()
        {
            Map argMap = new();

            foreach (string key in _arguments.Keys)
            {
                object value = _arguments[key];
                argMap[key] = value;
            }

            return argMap;
        }
    }

    public class AmqpBindingSpecification : BindingSpecification, IBindingSpecification
    {
        private readonly AmqpManagement _management;
        private readonly ITopologyListener _topologyListener;

        public AmqpBindingSpecification(AmqpManagement management)
        {
            _management = management;
            _topologyListener = ((IManagementTopology)_management).TopologyListener();
        }

        public Dictionary<string, object> Arguments()
        {
            return _arguments;
        }

        public string Path() => BindingsTarget();

        public async Task BindAsync()
        {
            var kv = new Map
        {
            { "source", _source },
            { "binding_key", _routingKey },
            { "arguments", ArgsToMap() },
            { _toQueue ? "destination_queue" : "destination_exchange", _destination }
        };

            string path = $"/{Consts.Bindings}";
            string method = AmqpManagement.Post;
            int[] expectedReturnCodes = new int[] { AmqpManagement.Code204 };

            // Note: must use await so that ConfigureAwait(false) can be called
            _topologyListener.BindingDeclared(this);

            await _management.RequestAsync(kv, path, method, expectedReturnCodes)
                .ConfigureAwait(false);
        }

        public async Task UnbindAsync()
        {
            string method = AmqpManagement.Delete;
            string destinationCharacter = _toQueue ? "dstq" : "dste";
            int[] expectedReturnCodes = new int[] { AmqpManagement.Code204 };

            if (_arguments.Count == 0)
            {
                string path =
                    $"/{Consts.Bindings}/src={Utils.EncodePathSegment(_source)};{($"{destinationCharacter}={Utils.EncodePathSegment(_destination)};key={Utils.EncodePathSegment(_routingKey)};args=")}";

                _topologyListener.BindingDeleted(Path());
                await _management.RequestAsync(null, path, method, expectedReturnCodes)
                    .ConfigureAwait(false);
            }
            else
            {
                string bindingsPath = BindingsTarget();
                List<Map> bindings = await GetBindings(bindingsPath).ConfigureAwait(false);
                string? path = MatchBinding(bindings, _routingKey, ArgsToMap());
                if (path is null)
                {
                    // TODO is this an error?
                }
                else
                {
                    _topologyListener.BindingDeclared(this);
                    await _management.RequestAsync(null, path, method, expectedReturnCodes)
                        .ConfigureAwait(false);
                }
            }
        }

        public IBindingSpecification SourceExchange(IExchangeSpecification exchangeSpec)
        {
            return SourceExchange(exchangeSpec.Name());
        }

        public IBindingSpecification SourceExchange(string exchangeName)
        {
            _toQueue = false;
            _source = exchangeName;
            return this;
        }

        public string SourceExchangeName()
        {
            return _source;
        }

        public IBindingSpecification DestinationQueue(IQueueSpecification queueSpec)
        {
            return DestinationQueue(queueSpec.Name());
        }

        public IBindingSpecification DestinationQueue(string queueName)
        {
            _toQueue = true;
            _destination = queueName;
            return this;
        }

        public string DestinationQueueName() => _destination;

        public IBindingSpecification DestinationExchange(IExchangeSpecification exchangeSpec)
        {
            return DestinationExchange(exchangeSpec.Name());
        }

        public IBindingSpecification DestinationExchange(string exchangeName)
        {
            _destination = exchangeName;
            return this;
        }

        public string DestinationExchangeName() => _destination;

        public IBindingSpecification Key(string key)
        {
            _routingKey = key;
            return this;
        }

        public string Key() => _routingKey;

        public IBindingSpecification Argument(string key, object value)
        {
            _arguments[key] = value;
            return this;
        }

        public IBindingSpecification Arguments(Dictionary<string, object> arguments)
        {
            _arguments = arguments;
            return this;
        }

        private string BindingsTarget()
        {
            string destinationField = _toQueue ? "dstq" : "dste";
            return "/bindings?src="
                   + Utils.EncodeHttpParameter(_source)
                   + "&"
                   + destinationField
                   + "="
                   + Utils.EncodeHttpParameter(_destination)
                   + "&key="
                   + Utils.EncodeHttpParameter(_routingKey);
        }

        private async Task<List<Map>> GetBindings(string path)
        {
            Amqp.Message result = await _management.RequestAsync(
                null, path,
                AmqpManagement.Get, new[] { AmqpManagement.Code200 }).ConfigureAwait(false);

            if (result.Body is not List list)
            {
                return new List<Map>();
            }

            var l = new List<Map>() { };
            foreach (object o in list)
            {
                if (o is Map item)
                {
                    l.Add(item);
                }
            }

            return l;
        }

        private string? MatchBinding(List<Map> bindings, string key, Map arguments)
        {
            string? uri = null;
            foreach (Map binding in bindings)
            {
                string bindingKey = (string)binding["binding_key"];
                Map bindingArguments = (Map)binding["arguments"];
                if ((key == null && bindingKey == null) || (key != null && key.Equals(bindingKey)))
                {
                    if ((arguments == null && bindingArguments == null) ||
                        (arguments != null && Utils.CompareMap(arguments, bindingArguments)))
                    {
                        uri = binding["location"].ToString();
                        break;
                    }
                }
            }

            return uri;
        }
    }
}
