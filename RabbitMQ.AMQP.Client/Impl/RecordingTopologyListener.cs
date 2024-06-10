using System.Collections.Concurrent;

namespace RabbitMQ.AMQP.Client.Impl;

public class RecordingTopologyListener : ITopologyListener
{
    private readonly ConcurrentDictionary<string, IQueueSpecification> _queueSpecifications = new();

    public void QueueDeclared(IQueueSpecification specification)
    {
        _queueSpecifications.TryAdd(specification.Name(), specification);
    }

    public void QueueDeleted(string name)
    {
        _queueSpecifications.TryRemove(name, out _);
    }

    public int QueueCount()
    {
        return _queueSpecifications.Count;
    }
}



internal class QueueSpec {

    public string Name { get; init;}
    // private  bool exclusive;
    
    public bool Exclusive
    {
        get;
        init;
    }
    
    // private  bool autoDelete;
    
    public bool AutoDelete
    {
        get;
        init;
    }
    
    // private Dictionary<string, object> arguments();
    
    public Dictionary<string, object> Arguments { get; init; } = new();

    public QueueSpec(AmqpQueueSpecification specification) {
        Name = specification.Name();
        Exclusive = specification.Exclusive();
        AutoDelete = specification.AutoDelete();
        // specification. (this.arguments::put);
    }

}