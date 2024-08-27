// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amqp;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl
{
    // TODO IEquatable
    public class DefaultQueueInfo : IQueueInfo
    {
        private readonly string _name;
        private readonly bool _durable;
        private readonly bool _autoDelete;
        private readonly bool _exclusive;
        private readonly QueueType _type;
        private readonly Dictionary<string, object> _arguments;
        private readonly string _leader;
        private readonly List<string> _replicas = new();
        private readonly ulong _messageCount;
        private readonly uint _consumerCount;


        internal DefaultQueueInfo(string queueName)
        {
            _name = queueName;
            _arguments = new Dictionary<string, object>();
            _leader = string.Empty;
            _replicas = new List<string>();
        }

        internal DefaultQueueInfo(Map response)
        {
            _name = (string)response["name"];
            _durable = (bool)response["durable"];
            _autoDelete = (bool)response["auto_delete"];
            _exclusive = (bool)response["exclusive"];
            var x = (QueueType)Enum.Parse(typeof(QueueType), ((string)response["type"]).ToUpperInvariant());
            _type = x;
            var m = (Map)response["arguments"];
            _arguments = m.Count == 0
                ? new Dictionary<string, object>()
                : m.ToDictionary(kv => (string)kv.Key, kv => kv.Value);

            _leader = (string)response["leader"];

            string[]? replicas = (string[])response["replicas"];
            if (replicas.Length > 0)
            {
                _replicas.AddRange(replicas);
            }

            _messageCount = (ulong)response["message_count"];
            _consumerCount = (uint)response["consumer_count"];
        }

        public string Name()
        {
            return _name;
        }

        public bool Durable()
        {
            return _durable;
        }

        public bool AutoDelete()
        {
            return _autoDelete;
        }

        public bool Exclusive()
        {
            return _exclusive;
        }

        public QueueType Type()
        {
            return _type;
        }

        public Dictionary<string, object> Arguments()
        {
            return _arguments;
        }

        public string Leader()
        {
            return _leader;
        }

        public List<string> Replicas()
        {
            return _replicas;
        }

        public ulong MessageCount()
        {
            return _messageCount;
        }

        public uint ConsumerCount()
        {
            return _consumerCount;
        }
    }

    /// <summary>
    /// AmqpQueueSpecification is a concrete implementation of IQueueSpecification
    /// It contains the necessary information to declare a queue on the broker
    /// </summary>
    public class AmqpQueueSpecification : IQueueSpecification
    {
        private readonly AmqpManagement _management;
        private readonly ITopologyListener _topologyListener;

        internal readonly TimeSpan _tenYears = TimeSpan.FromDays(365 * 10);

        private string? _name;
        private bool _exclusive = false;
        private bool _autoDelete = false;
        private const bool Durable = true;
        internal readonly Map _arguments = new();

        public AmqpQueueSpecification(AmqpManagement management)
        {
            _management = management;
            _topologyListener = ((IManagementTopology)_management).TopologyListener();
        }

        public IQueueSpecification Name(string name)
        {
            _name = name;
            return this;
        }

        public string Name()
        {
            return _name ?? "";
        }

        public IQueueSpecification Exclusive(bool exclusive)
        {
            _exclusive = exclusive;
            return this;
        }

        public bool Exclusive()
        {
            return _exclusive;
        }

        public bool AutoDelete()
        {
            return _autoDelete;
        }

        public IQueueSpecification AutoDelete(bool autoDelete)
        {
            _autoDelete = autoDelete;
            return this;
        }

        public IQueueSpecification Arguments(Dictionary<object, object> arguments)
        {
            foreach (object key in arguments.Keys)
            {
                object value = arguments[key];
                _arguments[key] = value;
            }

            return this;
        }

        public Dictionary<object, object> Arguments()
        {
            return _arguments;
        }

        public IQueueSpecification Type(QueueType type)
        {
            _arguments["x-queue-type"] = type.ToString().ToLower();
            return this;
        }

        public QueueType Type()
        {
            if (!_arguments.ContainsKey("x-queue-type"))
            {
                return QueueType.CLASSIC;
            }

            string type = (string)_arguments["x-queue-type"];
            return (QueueType)Enum.Parse(typeof(QueueType), type.ToUpperInvariant());
        }

        public IQueueSpecification DeadLetterExchange(string dlx)
        {
            _arguments["x-dead-letter-exchange"] = dlx;
            return this;
        }

        public IQueueSpecification DeadLetterRoutingKey(string dlrk)
        {
            _arguments["x-dead-letter-routing-key"] = dlrk;
            return this;
        }

        public IQueueSpecification OverflowStrategy(OverFlowStrategy overflow)
        {
            _arguments["x-overflow"] = overflow switch
            {
                OverFlowStrategy.DropHead => "drop-head",
                OverFlowStrategy.RejectPublish => "reject-publish",
                OverFlowStrategy.RejectPublishDlx => "reject-publish-dlx",
                _ => throw new ArgumentOutOfRangeException(nameof(overflow), overflow, null)
            };
            return this;
        }

        public IQueueSpecification MaxLengthBytes(ByteCapacity maxLengthBytes)
        {
            Utils.ValidatePositive("Max length", maxLengthBytes.ToBytes());
            _arguments["x-max-length-bytes"] = maxLengthBytes.ToBytes();
            return this;
        }

        public IQueueSpecification SingleActiveConsumer(bool singleActiveConsumer)
        {
            _arguments["x-single-active-consumer"] = singleActiveConsumer;
            return this;
        }

        public IQueueSpecification Expires(TimeSpan expiration)
        {
            Utils.ValidatePositive("Expiration", (long)expiration.TotalMilliseconds, (long)_tenYears.TotalMilliseconds);
            _arguments["x-expires"] = (long)expiration.TotalMilliseconds;
            return this;
        }

        public IStreamSpecification Stream()
        {
            Type(QueueType.STREAM);
            return new AmqpStreamSpecification(this);
        }

        public IQuorumQueueSpecification Quorum()
        {
            Type(QueueType.QUORUM);
            return new AmqpQuorumSpecification(this);
        }

        public IClassicQueueSpecification Classic()
        {
            Type(QueueType.CLASSIC);
            return new AmqpClassicSpecification(this);
        }

        public IQueueSpecification MaxLength(long maxLength)
        {
            Utils.ValidatePositive("Max length", maxLength);
            _arguments["x-max-length"] = maxLength;
            return this;
        }

        public IQueueSpecification MessageTtl(TimeSpan ttl)
        {
            Utils.ValidateNonNegative("TTL", (long)ttl.TotalMilliseconds, (long)_tenYears.TotalMilliseconds);
            _arguments["x-message-ttl"] = (long)ttl.TotalMilliseconds;
            return this;
        }

        public async Task<IQueueInfo> DeclareAsync()
        {
            if (Type() is QueueType.QUORUM or QueueType.STREAM)
            {
                // mandatory arguments for quorum queues and streams
                Exclusive(false).AutoDelete(false);
            }

            if (string.IsNullOrWhiteSpace(_name))
            {
                // If the name is not set, generate a random name
                // client side generated names are supported by the server
                // but here we generate a name to make easier to track the queue
                // and remove it later
                _name = Utils.GenerateQueueName();
            }

            var kv = new Map
        {
            { "durable", Durable },
            { "exclusive", _exclusive },
            { "auto_delete", _autoDelete },
            { "arguments", _arguments }
        };

            // TODO: encodePathSegment(queues)
            if (_name is null)
            {
                // TODO create "internal bug" exception type?
                throw new InvalidOperationException("_name is null, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
            }

            string path = $"/{Consts.Queues}/{Utils.EncodePathSegment(_name)}";
            string method = AmqpManagement.Put;
            int[] expectedResponseCodes = new int[] { AmqpManagement.Code200, AmqpManagement.Code201, AmqpManagement.Code409 };
            Message response = await _management.RequestAsync(kv, path, method, expectedResponseCodes)
                .ConfigureAwait(false);

            var result = new DefaultQueueInfo((Map)response.Body);
            _topologyListener.QueueDeclared(this);
            return result;
        }

        public async Task<IQueueInfo> DeleteAsync()
        {
            if (_name is null)
            {
                // TODO create "internal bug" exception type?
                throw new InvalidOperationException("_name is null or empty, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
            }

            if (string.IsNullOrEmpty(_name))
            {
                // TODO create "internal bug" exception type?
                throw new InvalidOperationException("_name is null or empty, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
            }

            string path = $"/{Consts.Queues}/{Utils.EncodePathSegment(_name)}";
            string method = AmqpManagement.Delete;
            int[] expectedResponseCodes = new int[] { AmqpManagement.Code200 };
            await _management.RequestAsync(null, path, method, expectedResponseCodes)
                .ConfigureAwait(false);

            _topologyListener.QueueDeleted(_name);
            return new DefaultQueueInfo(_name);
        }
    }

    public class AmqpStreamSpecification : IStreamSpecification
    {
        private readonly AmqpQueueSpecification _parent;

        public AmqpStreamSpecification(AmqpQueueSpecification parent)
        {
            _parent = parent;
        }

        public IStreamSpecification MaxAge(TimeSpan maxAge)
        {
            Utils.ValidatePositive("x-max-age", (long)maxAge.TotalMilliseconds,
                (long)_parent._tenYears.TotalMilliseconds);
            _parent._arguments["x-max-age"] = $"{maxAge.Seconds}s";
            return this;
        }

        public IStreamSpecification MaxSegmentSizeBytes(ByteCapacity maxSegmentSize)
        {
            Utils.ValidatePositive("x-stream-max-segment-size-bytes", maxSegmentSize.ToBytes());
            _parent._arguments["x-stream-max-segment-size-bytes"] = maxSegmentSize.ToBytes();
            return this;
        }

        public IStreamSpecification InitialClusterSize(int initialClusterSize)
        {
            Utils.ValidatePositive("x-initial-cluster-size", initialClusterSize);
            _parent._arguments["x-initial-cluster-size"] = initialClusterSize;
            return this;
        }

        public IQueueSpecification Queue()
        {
            return _parent;
        }
    }

    public class AmqpQuorumSpecification : IQuorumQueueSpecification
    {
        private readonly AmqpQueueSpecification _parent;

        public AmqpQuorumSpecification(AmqpQueueSpecification parent)
        {
            _parent = parent;
        }

        public IQuorumQueueSpecification DeadLetterStrategy(QuorumQueueDeadLetterStrategy strategy)
        {
            _parent._arguments["x-dead-letter-strategy"] = strategy switch
            {
                QuorumQueueDeadLetterStrategy.AtMostOnce => "at-most-once",
                QuorumQueueDeadLetterStrategy.AtLeastOnce => "at-least-once",
                _ => throw new ArgumentOutOfRangeException(nameof(strategy), strategy, null)
            };
            return this;
        }

        public IQuorumQueueSpecification DeliveryLimit(int limit)
        {
            Utils.ValidatePositive("x-max-delivery-limit", limit);
            _parent._arguments["x-max-delivery-limit"] = limit;
            return this;
        }

        public IQuorumQueueSpecification QuorumInitialGroupSize(int size)
        {
            Utils.ValidatePositive("x-quorum-initial-group-size", size);
            _parent._arguments["x-quorum-initial-group-size"] = size;
            return this;
        }

        public IQueueSpecification Queue()
        {
            return _parent;
        }
    }

    public class AmqpClassicSpecification : IClassicQueueSpecification
    {
        private readonly AmqpQueueSpecification _parent;

        public AmqpClassicSpecification(AmqpQueueSpecification parent)
        {
            _parent = parent;
        }

        public IClassicQueueSpecification MaxPriority(int maxPriority)
        {
            Utils.ValidatePositive("x-max-priority", maxPriority, 255);
            _parent._arguments["x-max-priority"] = maxPriority;
            return this;
        }

        public IClassicQueueSpecification Mode(ClassicQueueMode mode)
        {
            _parent._arguments["x-queue-mode"] = mode switch
            {
                ClassicQueueMode.Default => "default",
                ClassicQueueMode.Lazy => "lazy",
                _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null)
            };
            return this;
        }

        public IClassicQueueSpecification Version(ClassicQueueVersion version)
        {
            _parent._arguments["x-queue-version"] = version switch
            {
                ClassicQueueVersion.V1 => 1,
                ClassicQueueVersion.V2 => 2,
                _ => throw new ArgumentOutOfRangeException(nameof(version), version, null)
            };
            return this;
        }

        public IQueueSpecification Queue()
        {
            return _parent;
        }
    }
}
