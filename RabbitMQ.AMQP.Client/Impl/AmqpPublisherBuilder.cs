namespace RabbitMQ.AMQP.Client.Impl;

public class AddressBuilder : IAddressBuilder<AddressBuilder>
{
    private string? _exchange;

    private string? _queue;

    private string? _key;

    public AddressBuilder Exchange(IExchangeSpecification exchangeSpec)
    {
        return Exchange(exchangeSpec.Name());
    }

    public AddressBuilder Exchange(string? exchange)
    {
        _exchange = exchange;
        return this;
    }

    public AddressBuilder Queue(IQueueSpecification queueSpec)
    {
        return Queue(queueSpec.Name());
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
        if (_exchange == null && _queue == null)
        {
            throw new InvalidAddressException("Exchange or Queue must be set");
        }

        if (_exchange != null && _queue != null)
        {
            throw new InvalidAddressException("Exchange and Queue cannot be set together");
        }

        if (_exchange != null)
        {
            if (string.IsNullOrEmpty(_exchange))
            {
                throw new InvalidAddressException("Exchange must be set");
            }

            if (!string.IsNullOrEmpty(_key))
            {
                return $"/{Consts.Exchanges}/{Utils.EncodePathSegment(_exchange)}/{Utils.EncodePathSegment(_key)}";
            }

            return $"/{Consts.Exchanges}/{Utils.EncodePathSegment(_exchange)}";
        }

        if (_queue == null)
        {
            return "";
        }

        if (string.IsNullOrEmpty(_queue))
        {
            throw new InvalidAddressException("Queue must be set");
        }

        return $"/{Consts.Queues}/{Utils.EncodePathSegment(_queue)}";
    }
}

public class AmqpPublisherBuilder(AmqpConnection connection) : IPublisherBuilder
{
    private string? _exchange;
    private string? _key;
    private string? _queue;
    private TimeSpan _timeout = TimeSpan.FromSeconds(10);
    private int _maxInFlight = 1000;


    public IPublisherBuilder Exchange(IExchangeSpecification exchangeSpec)
    {
        return Exchange(exchangeSpec.Name());
    }

    public IPublisherBuilder Exchange(string exchangeName)
    {
        _exchange = exchangeName;
        return this;
    }

    public IPublisherBuilder Queue(IQueueSpecification queueSpec)
    {
        return Queue(queueSpec.Name());
    }

    public IPublisherBuilder Queue(string queueName)
    {
        _queue = queueName;
        return this;
    }

    public IPublisherBuilder Key(string key)
    {
        _key = key;
        return this;
    }

    public IPublisherBuilder PublishTimeout(TimeSpan timeout)
    {
        _timeout = timeout;
        return this;
    }

    public IPublisherBuilder MaxInflightMessages(int maxInFlight)
    {
        _maxInFlight = maxInFlight;
        return this;
    }

    public async Task<IPublisher> BuildAsync(CancellationToken cancellationToken = default)
    {
        string address = new AddressBuilder().Exchange(_exchange).Queue(_queue).Key(_key).Address();

        AmqpPublisher publisher = new(connection, address, _timeout, _maxInFlight);

        // TODO pass cancellationToken
        await publisher.OpenAsync()
            .ConfigureAwait(false);

        return publisher;
    }
}
