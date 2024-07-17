using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpConsumerBuilder(AmqpConnection connection) : IConsumerBuilder
{
    private string _queue = "";
    private int _initialCredits = 1;
    private Map _filters = new Map();


    public IConsumerBuilder Queue(string queue)
    {
        _queue = queue;
        return this;
    }


    private MessageHandler _handler = (message, context) => { };

    public IConsumerBuilder MessageHandler(MessageHandler handler)
    {
        _handler = handler;
        return this;
    }

    public IConsumerBuilder InitialCredits(int initialCredits)
    {
        _initialCredits = initialCredits;
        return this;
    }

    public IConsumerBuilder.IStreamOptions Stream()
    {
        return new DefaultStreamOptions(this, _filters);
    }


    public IConsumer Build()
    {
        return new AmqpConsumer(connection, new AddressBuilder().Queue(_queue).Address(),
            _handler, _initialCredits, _filters);
    }
}

public class DefaultStreamOptions(IConsumerBuilder builder, Map filters)
    : IConsumerBuilder.IStreamOptions
{
    public IConsumerBuilder.IStreamOptions Offset(long offset)
    {
        filters[new Symbol("rabbitmq:stream-offset-spec")] = offset;
        return this;
    }

    // public IConsumerBuilder.IStreamOptions Offset(Instant timestamp)
    // {
    //     notNull(timestamp, "Timestamp offset cannot be null");
    //     this.offsetSpecification(JSType.Date.from(timestamp));
    //     return this;
    // }

    public IConsumerBuilder.IStreamOptions Offset(StreamOffsetSpecification specification)
    {
        // notNull(specification, "Offset specification cannot be null");
        OffsetSpecification(specification.ToString().ToLower());
        return this;
    }


    public IConsumerBuilder.IStreamOptions Offset(string interval)
    {
        // notNull(interval, "Interval offset cannot be null");
        // if (!Utils.validateMaxAge(interval))
        // {
        //     throw new IllegalArgumentException(
        //         "Invalid value for interval: "
        //         + interval
        //         + ". "
        //         + "Valid examples are: 1Y, 7D, 10m. See https://www.rabbitmq.com/docs/streams#retention.");
        // }

        OffsetSpecification(interval);
        return this;
    }


    public IConsumerBuilder.IStreamOptions FilterValues(string[] values)
    {
        filters[new Symbol("rabbitmq:stream-filter")] = values.ToList();
        return this;
    }


    public IConsumerBuilder.IStreamOptions FilterMatchUnfiltered(bool matchUnfiltered)
    {
        filters[new Symbol("rabbitmq:stream-match-unfiltered")] = matchUnfiltered;
        return this;
    }


    public IConsumerBuilder Builder()
    {
        return builder;
    }

    private void OffsetSpecification(object value)
    {
        filters[new Symbol("rabbitmq:stream-offset-spec")] = value;
    }
}
