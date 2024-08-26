using System.Collections.Concurrent;

namespace RabbitMQ.AMQP.Client.Impl;

public interface IVisitor
{
    Task VisitQueuesAsync(IEnumerable<QueueSpec> queueSpec);
    Task VisitExchangesAsync(IEnumerable<ExchangeSpec> exchangeSpec);

    Task VisitBindingsAsync(IEnumerable<BindingSpec> bindingSpec);
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

    private readonly ConcurrentDictionary<string, ExchangeSpec> _exchangeSpecifications = new();

    private readonly ConcurrentDictionary<string, BindingSpec> _bindingSpecifications = new();

    private void RemoveBindingsSpecificationFromQueue(string queueName)
    {
        foreach (var binding in _bindingSpecifications.Values)
        {
            if (binding.DestinationQueue == queueName)
            {
                _bindingSpecifications.TryRemove(binding.Path, out _);
            }
        }
    }

    private void RemoveBindingsSpecificationFromExchange(string exchangeName)
    {
        foreach (var binding in _bindingSpecifications.Values)
        {
            if (binding.SourceExchange == exchangeName)
            {
                _bindingSpecifications.TryRemove(binding.Path, out _);
            }
        }
    }


    public void QueueDeclared(IQueueSpecification specification)
    {
        _queueSpecifications.TryAdd(specification.Name(), new QueueSpec(specification));
    }

    public void QueueDeleted(string name)
    {
        _queueSpecifications.TryRemove(name, out _);
        RemoveBindingsSpecificationFromQueue(name);
    }

    public void ExchangeDeclared(IExchangeSpecification specification)
    {
        _exchangeSpecifications.TryAdd(specification.Name(), new ExchangeSpec(specification));
    }

    public void ExchangeDeleted(string name)
    {
        _exchangeSpecifications.TryRemove(name, out _);
        RemoveBindingsSpecificationFromExchange(name);
    }

    public void BindingDeclared(IBindingSpecification specification)
    {
        _bindingSpecifications.TryAdd(specification.Path(), new BindingSpec(specification));
    }

    public void BindingDeleted(string key)
    {
        _bindingSpecifications.TryRemove(key, out _);
    }

    public void Clear()
    {
        _queueSpecifications.Clear();
        _exchangeSpecifications.Clear();
        _bindingSpecifications.Clear();
    }

    public int QueueCount()
    {
        return _queueSpecifications.Count;
    }

    public int ExchangeCount()
    {
        return _exchangeSpecifications.Count;
    }

    public int BindingCount() => _bindingSpecifications.Count;


    public async Task Accept(IVisitor visitor)
    {
        await visitor.VisitQueuesAsync(_queueSpecifications.Values).ConfigureAwait(false);

        await visitor.VisitExchangesAsync(_exchangeSpecifications.Values).ConfigureAwait(false);

        await visitor.VisitBindingsAsync(_bindingSpecifications.Values).ConfigureAwait(false);
    }
}

// TODO this could probably be made internal
public class QueueSpec(IQueueSpecification specification)
{
    public string Name { get; init; } = specification.Name();

    public bool Exclusive { get; init; } = specification.Exclusive();

    public bool AutoDelete { get; init; } = specification.AutoDelete();

    public Dictionary<object, object> Arguments { get; init; } = specification.Arguments();
}

public class ExchangeSpec(IExchangeSpecification specification)
{
    public string Name { get; } = specification.Name();

    public ExchangeType Type { get; } = specification.Type();

    public bool AutoDelete { get; } = specification.AutoDelete();

    public Dictionary<string, object> Arguments { get; } = specification.Arguments();
}

public class BindingSpec(IBindingSpecification specification)
{
    public string SourceExchange { get; } = specification.SourceExchangeName();

    public string DestinationQueue { get; } = specification.DestinationQueueName();

    public string DestinationExchange { get; } = specification.DestinationExchangeName();

    public string Key { get; } = specification.Key();

    public Dictionary<string, object> Arguments { get; } = specification.Arguments();

    public string Path { get; } = specification.Path();
}
