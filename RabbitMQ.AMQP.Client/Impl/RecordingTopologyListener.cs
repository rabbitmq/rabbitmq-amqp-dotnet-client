using System.Collections.Concurrent;

namespace RabbitMQ.AMQP.Client.Impl;

public interface IVisitor
{
    Task VisitQueues(List<QueueSpec> queueSpec);
}

/// <summary>
/// RecordingTopologyListener is a concrete implementation of <see cref="ITopologyListener"/>
///  It is used to record the topology of the entities declared in the AMQP server ( like queues, exchanges, etc)
/// It is used to recover the topology of the server after a connection is established in case of a reconnection
/// Each time am entity is declared or deleted, the listener will record the event
/// </summary>
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

    public void Clear()
    {
        _queueSpecifications.Clear();
    }

    public int QueueCount()
    {
        return _queueSpecifications.Count;
    }

    public async Task Accept(IVisitor visitor)
    {
        await visitor.VisitQueues(_queueSpecifications.Values.ToList());
    }
}

public class QueueSpec(IQueueSpecification specification)
{
    public string Name { get; init; } = specification.Name();

    public bool Exclusive { get; init; } = specification.Exclusive();

    public bool AutoDelete { get; init; } = specification.AutoDelete();
    

    public Dictionary<object, object> Arguments { get; init; } = specification.Arguments();
}