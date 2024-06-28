namespace RabbitMQ.AMQP.Client;

public interface IPublisherBuilder : IAddressBuilder<IPublisherBuilder>
{
    IPublisherBuilder PublishTimeout(TimeSpan timeout);

    IPublisherBuilder MaxInFlight(int maxInFlight);
    IPublisher Build();
}