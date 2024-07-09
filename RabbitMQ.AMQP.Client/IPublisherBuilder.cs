namespace RabbitMQ.AMQP.Client;

public interface IPublisherBuilder : IAddressBuilder<IPublisherBuilder>
{
    IPublisherBuilder PublishTimeout(TimeSpan timeout);

    IPublisherBuilder MaxInflightMessages(int maxInFlight);
    IPublisher Build();
}
