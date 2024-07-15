namespace RabbitMQ.AMQP.Client;

public interface IEntityInfo
{
}

/// <summary>
/// Generic interface for declaring entities with result of type T
/// </summary>
/// <typeparam name="T"></typeparam>
public interface IEntityInfoDeclaration<T> where T : IEntityInfo
{
    Task<T> Declare();
}

/// <summary>
/// Generic interface for declaring entities without result
/// </summary>
public interface IEntityDeclaration
{
    Task Declare();
}

public interface IQueueSpecification : IEntityInfoDeclaration<IQueueInfo>
{
    IQueueSpecification Name(string name);
    public string Name();

    IQueueSpecification Exclusive(bool exclusive);
    public bool Exclusive();

    IQueueSpecification AutoDelete(bool autoDelete);
    public bool AutoDelete();

    IQueueSpecification Arguments(Dictionary<object, object> arguments);
    public Dictionary<object, object> Arguments();

    IQueueSpecification Type(QueueType type);
    public QueueType Type();

    // IQuorumQueueSpecification Quorum();
}

// public interface IQuorumQueueSpecification 
// {
//     IQueueSpecification Queue();
// }

public interface IQueueDeletion
{
    // TODO consider returning a QueueStatus object with some info after deletion
    Task<IEntityInfo> Delete(string name);
}

public interface IExchangeSpecification : IEntityDeclaration
{
    IExchangeSpecification Name(string name);

    IExchangeSpecification AutoDelete(bool autoDelete);

    IExchangeSpecification Type(ExchangeType type);

    IExchangeSpecification Type(string type); // TODO: Add this

    IExchangeSpecification Argument(string key, object value);
}

public interface IExchangeDeletion
{
    // TODO consider returning a ExchangeStatus object with some info after deletion
    Task Delete(string name);
}

public interface IBindingSpecification
{
    IBindingSpecification SourceExchange(string exchange);

    IBindingSpecification DestinationQueue(string queue);

    IBindingSpecification DestinationExchange(string exchange);

    IBindingSpecification Key(string key);

    IBindingSpecification Argument(string key, object value);

    IBindingSpecification Arguments(Dictionary<string, object> arguments);

    Task Bind();
    Task Unbind();
}
