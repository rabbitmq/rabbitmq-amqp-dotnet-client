using System.Collections.Concurrent;

namespace RabbitMQ.AMQP.Client.Impl;

public interface IVisitor
{
    void VisitQueues(List<QueueSpec> queueSpec);
}

public class RecordingTopologyListener : ITopologyListener
{
    private readonly ConcurrentDictionary<string, QueueSpec> _queueSpecifications = new();

    public void QueueDeclared(IQueueSpecification specification)
    {
        _queueSpecifications.TryAdd(specification.Name(), new QueueSpec(specification));
    }

    public void QueueDeleted(string name)
    {
        _queueSpecifications.TryRemove(name, out _);
    }

    public int QueueCount()
    {
        return _queueSpecifications.Count;
    }

    public void Accept(IVisitor visitor)
    {
        visitor.VisitQueues(_queueSpecifications.Values.ToList());
    }
}

public class QueueSpec(IQueueSpecification specification)
{
    public string Name { get; init; } = specification.Name();

    public bool Exclusive { get; init; } = specification.Exclusive();

    public bool AutoDelete { get; init; } = specification.AutoDelete();
    

    public Dictionary<object, object> Arguments { get; init; } = specification.Arguments();
}