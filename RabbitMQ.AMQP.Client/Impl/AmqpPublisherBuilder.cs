namespace RabbitMQ.AMQP.Client.Impl;

public class AddressBuilder : IAddressBuilder<AddressBuilder>
{
    private string? _exchange;

    private string? _queue;

    private string? _key;

    public AddressBuilder Exchange(string? exchange)
    {
        _exchange = exchange;
        return this;
    }

    public AddressBuilder Queue(string? queue)
    {
        _queue = queue;
        return this;
    }

    public AddressBuilder Key(string? key)
    {
        _key = key;
        return this;
    }

    public string Address()
    {
        if (_exchange == null && _queue == null) throw new InvalidAddressException("Exchange or Queue must be set");

        if (_exchange != null && _queue != null)
            throw new InvalidAddressException("Exchange and Queue cannot be set together");

        if (_exchange != null)
        {
            if (string.IsNullOrEmpty(_exchange)) throw new InvalidAddressException("Exchange must be set");

            if (!string.IsNullOrEmpty(_key)) return "/exchange/" + _exchange + "/key/" + _key;
            return "/exchange/" + _exchange;
        }

        if (_queue == null) return "";
        if (string.IsNullOrEmpty(_queue)) throw new InvalidAddressException("Queue must be set");
        return "/queue/" + _queue;
    }
}

public class AmqpPublisherBuilder(AmqpConnection connection) : IPublisherBuilder
{
    private string? _exchange;
    private string? _key;
    private string? _queue;


    public IPublisherBuilder Exchange(string exchange)
    {
        _exchange = exchange;
        return this;
    }

    public IPublisherBuilder Queue(string queue)
    {
        _queue = queue;
        return this;
    }

    public IPublisherBuilder Key(string key)
    {
        _key = key;
        return this;
    }

    public IPublisherBuilder PublishTimeout(TimeSpan timeout)
    {
        throw new NotImplementedException();
    }

    public IPublisher Build()
    {
        var newPublisher = new AmqpPublisher(connection,
            new AddressBuilder().Exchange(_exchange).Queue(_queue).Key(_key).Address());
        connection.Publishers.TryAdd(newPublisher.Id, newPublisher);
        return newPublisher;
    }
}