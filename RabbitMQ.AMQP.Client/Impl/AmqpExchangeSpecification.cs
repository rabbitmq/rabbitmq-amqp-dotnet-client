using Amqp;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpExchangeInfo : IExchangeInfo
{
}

public class AmqpExchangeSpecification(AmqpManagement management) : IExchangeSpecification
{
    private string _name = "";
    private bool _autoDelete;
    private ExchangeType _type;
    private string _typeString = "";
    private readonly Map _arguments = new();


    // Map<String, Object> body = new LinkedHashMap<>();
    // body.put("type", this.type);
    // body.put("durable", DURABLE);
    // body.put("auto_delete", this.autoDelete);
    // body.put("internal", INTERNAL);
    // body.put("arguments", this.arguments);


    public async Task<IExchangeInfo> Declare()
    {
        if (string.IsNullOrEmpty(_name))
        {
            throw new ArgumentException("Exchange name must be set");
        }


        var kv = new Map
        {
            { "auto_delete", _autoDelete },
            { "arguments", _arguments },
            { "type", _type.ToString().ToLower() },
            { "durable", true }
        };

        // TODO: encodePathSegment(queues)
        // Message request = await management.Request(kv, $"/{Consts.Exchanges}/{_name}",
        // for the moment we won't use the message response
        await management.Request(kv, $"/{Consts.Exchanges}/{_name}",
            AmqpManagement.Put,
            [
                AmqpManagement.Code204,
                AmqpManagement.Code201,
                AmqpManagement.Code409
            ]).ConfigureAwait(false);

        return new AmqpExchangeInfo();
    }

    public IExchangeSpecification Name(string name)
    {
        _name = name;
        return this;
    }

    public IExchangeSpecification AutoDelete(bool autoDelete)
    {
        _autoDelete = autoDelete;
        return this;
    }

    public IExchangeSpecification Type(ExchangeType type)
    {
        _type = type;
        return this;
    }

    public IExchangeSpecification Type(string type)
    {
        _typeString = type;
        return this;
    }

    public IExchangeSpecification Argument(string key, object value)
    {
        _arguments[key] = value;
        return this;
    }
}
