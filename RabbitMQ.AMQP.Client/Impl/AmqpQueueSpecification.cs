using Amqp;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl;

public class DefaultQueueInfo : IQueueInfo
{
    private readonly string _name;
    private readonly bool _durable;
    private readonly bool _autoDelete;
    private readonly bool _exclusive;
    private readonly QueueType _type;
    private readonly Dictionary<string, object> _arguments;
    private readonly string _leader;
    private readonly List<string> _replicas;
    private readonly ulong _messageCount;
    private readonly uint _consumerCount;


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
        _replicas = replicas.Length == 0 ? [] : [.. replicas];
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
/// <param name="management"></param>
public class AmqpQueueSpecification(AmqpManagement management) : IQueueSpecification
{
    internal readonly TimeSpan _tenYears = TimeSpan.FromDays(365 * 10);

    private string? _name;
    private bool _exclusive = false;
    private bool _autoDelete = false;
    private const bool Durable = true;
    internal readonly Map _arguments = new();

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
        foreach ((object key, object value) in arguments)
        {
            _arguments[key] = value;
        }

        return this;
    }

    public Dictionary<object, object> Arguments()
    {
        var result = new Dictionary<object, object>();
        foreach ((object? key, object? value) in _arguments)
        {
            result[key] = value;
        }

        return result;
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

    public async Task<IQueueInfo> Declare()
    {
        if (Type() is QueueType.QUORUM or QueueType.STREAM)
        {
            // mandatory arguments for quorum queues and streams
            Exclusive(false).AutoDelete(false);
        }

        if (string.IsNullOrEmpty(_name) || _name.Trim() == "")
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
        Message request = await management.Request(kv, $"/{Consts.Queues}/{Utils.EncodePathSegment(_name)}",
            AmqpManagement.Put,
            [
                AmqpManagement.Code200,
                AmqpManagement.Code201,
                AmqpManagement.Code409
            ]).ConfigureAwait(false);

        var result = new DefaultQueueInfo((Map)request.Body);
        management.TopologyListener().QueueDeclared(this);
        return result;
    }
}

public class AmqpStreamSpecification(AmqpQueueSpecification parent) : IStreamSpecification
{
    public IStreamSpecification MaxAge(TimeSpan maxAge)
    {
        Utils.ValidatePositive("x-max-age", (long)maxAge.TotalMilliseconds,
            (long)parent._tenYears.TotalMilliseconds);
        parent._arguments["x-max-age"] = $"{maxAge.Seconds}s";
        return this;
    }

    public IStreamSpecification MaxSegmentSizeBytes(ByteCapacity maxSegmentSize)
    {
        Utils.ValidatePositive("x-stream-max-segment-size-bytes", maxSegmentSize.ToBytes());
        parent._arguments["x-stream-max-segment-size-bytes"] = maxSegmentSize.ToBytes();
        return this;
    }

    public IStreamSpecification InitialClusterSize(int initialClusterSize)
    {
        Utils.ValidatePositive("x-initial-cluster-size", initialClusterSize);
        parent._arguments["x-initial-cluster-size"] = initialClusterSize;
        return this;
    }

    public IQueueSpecification Queue()
    {
        return parent;
    }
}

public class AmqpQuorumSpecification(AmqpQueueSpecification parent) : IQuorumQueueSpecification
{
    public IQuorumQueueSpecification DeadLetterStrategy(QuorumQueueDeadLetterStrategy strategy)
    {
        parent._arguments["x-dead-letter-strategy"] = strategy switch
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
        parent._arguments["x-max-delivery-limit"] = limit;
        return this;
    }

    public IQuorumQueueSpecification QuorumInitialGroupSize(int size)
    {
        Utils.ValidatePositive("x-quorum-initial-group-size", size);
        parent._arguments["x-quorum-initial-group-size"] = size;
        return this;
    }

    public IQueueSpecification Queue()
    {
        return parent;
    }
}

public class AmqpClassicSpecification(AmqpQueueSpecification parent) : IClassicQueueSpecification
{
    public IClassicQueueSpecification MaxPriority(int maxPriority)
    {
        Utils.ValidatePositive("x-max-priority", maxPriority, 255);
        parent._arguments["x-max-priority"] = maxPriority;
        return this;
    }

    public IClassicQueueSpecification Mode(ClassicQueueMode mode)
    {
        parent._arguments["x-queue-mode"] = mode switch
        {
            ClassicQueueMode.Default => "default",
            ClassicQueueMode.Lazy => "lazy",
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null)
        };
        return this;
    }

    public IClassicQueueSpecification Version(ClassicQueueVersion version)
    {
        parent._arguments["x-queue-version"] = version switch
        {
            ClassicQueueVersion.V1 => 1,
            ClassicQueueVersion.V2 => 2,
            _ => throw new ArgumentOutOfRangeException(nameof(version), version, null)
        };
        return this;
    }

    public IQueueSpecification Queue()
    {
        return parent;
    }
}

public class DefaultQueueDeletionInfo : IEntityInfo
{
}

public class AmqpQueueDeletion(AmqpManagement management) : IQueueDeletion
{
    public async Task<IEntityInfo> Delete(string name)
    {
        await management
            .Request(null, $"/{Consts.Queues}/{Utils.EncodePathSegment(name)}", AmqpManagement.Delete,
                new[] { AmqpManagement.Code200, })
            .ConfigureAwait(false);

        management.TopologyListener().QueueDeleted(name);
        return new DefaultQueueDeletionInfo();
    }
}
