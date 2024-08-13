using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpExchangeSpecification(AmqpManagement management) : IExchangeSpecification
{
    private string _name = "";
    private bool _autoDelete;
    private ExchangeType _type = ExchangeType.DIRECT;
    private string _typeString = ""; // TODO: add this
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
            { "type", _type.ToString().ToLower() },
            { "durable", true }
        };

        // TODO: encodePathSegment(queues)
        // Message request = await management.Request(kv, $"/{Consts.Exchanges}/{_name}",
        // for the moment we won't use the message response
        string path = $"/{Consts.Exchanges}/{Utils.EncodePathSegment(_name)}";
        string method = AmqpManagement.Put;
        int[] expectedResponseCodes = [AmqpManagement.Code204, AmqpManagement.Code201, AmqpManagement.Code409];
        return management.RequestAsync(kv, path, method, expectedResponseCodes);
    }

    public Task DeleteAsync()
    {
        string path = $"/{Consts.Exchanges}/{Utils.EncodePathSegment(_name)}";
        string method = AmqpManagement.Delete;
        int[] expectedResponseCodes = [AmqpManagement.Code204];
        // TODO management topology listener?
        return management.RequestAsync(null, path, method, expectedResponseCodes);
    }

    public string Name()
    {
        return _name;
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
