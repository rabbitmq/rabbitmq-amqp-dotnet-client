namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpConsumerBuilder(AmqpConnection connection) : IConsumerBuilder
{
    private string _queue = "";
    private int _initialCredits = 1;


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

    public IConsumerBuilder.IStreamOptions Stream() => throw new NotImplementedException();


    public IConsumer Build()
    {
        return new AmqpConsumer(connection, new AddressBuilder().Queue(_queue).Address(),
            _handler, _initialCredits);
    }
}
