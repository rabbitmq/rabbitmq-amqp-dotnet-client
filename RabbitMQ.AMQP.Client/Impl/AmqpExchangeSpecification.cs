namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpExchangeSpecification : IExchangeSpecification
{
    
    private string _name = "";
    private bool _autoDelete;
    private ExchangeType _type;
    private string _typeString = "";
    
    
    public Task<IExchangeInfo> Declare() => throw new NotImplementedException();

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

    public IExchangeSpecification Argument(string key, object value) => throw new NotImplementedException();
}
