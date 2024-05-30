using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client;

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
        this._durable = (bool)response["durable"];
        this._autoDelete = (bool)response["auto_delete"];
        this._exclusive = (bool)response["exclusive"];
        var x = (QueueType)Enum.Parse(typeof(QueueType), ((string)response["type"]).ToUpperInvariant());
        this._type = x;
        var m = (Map)response["arguments"];
        this._arguments = m.Count == 0
            ? new Dictionary<string, object>()
            : m.ToDictionary(kv => (string)kv.Key, kv => kv.Value);

        this._leader = (string)response["leader"];
        var replicas = (string[])response["replicas"];
        this._replicas = replicas.Length == 0 ? [] : [..replicas];
        this._messageCount = ((ulong)response["message_count"]);
        this._consumerCount = ((uint)response["consumer_count"]);
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

public class AmqpQueueSpecification(AmqpManagement management) : IQueueSpecification
{
    private string? _name;
    private bool _exclusive = false;
    private bool _autoDelete = false;
    private bool _durable = false;


    public IQueueSpecification Name(string name)
    {
        _name = name;
        return this;
    }


    public IQueueSpecification Exclusive(bool exclusive)
    {
        _exclusive = exclusive;
        return this;
    }

    public IQueueSpecification AutoDelete(bool autoDelete)
    {
        _autoDelete = autoDelete;
        return this;
    }

    public IQueueSpecification Durable(bool durable)
    {
        _durable = durable;
        return this;
    }

    public async Task<IQueueInfo> Declare()
    {
        if (_name == null)
        {
            throw new InvalidOperationException("Queue name is required");
        }

        var kv = new Map
        {
            { "durable", _durable },
            { "exclusive", _exclusive },
            { "auto_delete", _autoDelete }
        };
        var result = await management.Request(kv, $"/queues/{_name}",
            AmqpManagement.Put, new[]
            {
                AmqpManagement.Code200,
                AmqpManagement.Code201
            });

        return new DefaultQueueInfo((Map)result.Body);
    }
}