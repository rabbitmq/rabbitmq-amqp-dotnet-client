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