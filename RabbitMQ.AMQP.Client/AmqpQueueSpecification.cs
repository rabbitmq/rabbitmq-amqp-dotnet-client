using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client;

public class AmqpQueueSpecification(AmqpManagement management) : IQueueSpecification
{
    private string? _name;
    private bool _exclusive = false;
    private bool _autoDelete = false;
    private bool _durable = false;


    private AmqpManagement _management = management;


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

    public Task Declare()
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
        var message = new Message(kv);
        message.Properties = new Properties
        {
            MessageId = "0",
            To = $"/queues/{_name}",
            Subject = "PUT",
            ReplyTo = "$me"
        };

        return _management.SendAsync(message);
    }
}