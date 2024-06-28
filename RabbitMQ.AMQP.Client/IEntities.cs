namespace RabbitMQ.AMQP.Client;

public interface IEntityInfo
{
}


/// <summary>
/// Generic interface for declaring entities
/// </summary>
/// <typeparam name="T"></typeparam>
public interface IEntityDeclaration<T> where T : IEntityInfo
{
    Task<T> Declare();
}

public interface IQueueSpecification : IEntityDeclaration<IQueueInfo>
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

