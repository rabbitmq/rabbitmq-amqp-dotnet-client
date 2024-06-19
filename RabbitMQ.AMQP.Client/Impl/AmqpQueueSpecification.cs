using Amqp;
using Amqp.Framing;
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
        var replicas = (string[])response["replicas"];
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
    private string? _name;
    private bool _exclusive = false;
    private bool _autoDelete = false;
    private const bool Durable = true;
    private readonly Map _arguments = new();

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
        foreach (var (key, value) in arguments)
        {
            _arguments[key] = value;
        }

        return this;
    }

    public Dictionary<object, object> Arguments()
    {
        var result = new Dictionary<object, object>();
        foreach (var (key, value) in _arguments)
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
        if (!_arguments.ContainsKey("x-queue-type")) return QueueType.CLASSIC;
        var type = (string)_arguments["x-queue-type"];
        return (QueueType)Enum.Parse(typeof(QueueType), type.ToUpperInvariant());
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
        var request = await management.Request(kv, $"/queues/{_name}",
            AmqpManagement.Put, new[]
            {
                AmqpManagement.Code200,
                AmqpManagement.Code201,
                AmqpManagement.Code409
            });

        var result = new DefaultQueueInfo((Map)request.Body);
        management.TopologyListener().QueueDeclared(this);
        return result;
    }
}

public class DefaultQueueDeletionInfo : IEntityInfo
{
}

public class AmqpQueueDeletion(AmqpManagement management) : IQueueDeletion
{
    public async Task<IEntityInfo> Delete(string name)
    {
        await management.Request(null, $"/queues/{name}", AmqpManagement.Delete, new[]
        {
            AmqpManagement.Code200,
        });
        management.TopologyListener().QueueDeleted(name);
        return new DefaultQueueDeletionInfo();
    }
}