namespace RabbitMQ.AMQP.Client;

public class PublisherException(string message) : Exception(message);

public enum OutcomeState
{
    Accepted,
    Failed,
}

public class OutcomeDescriptor(ulong code, string description, OutcomeState state, Error? error)
{
    public OutcomeState State { get; internal set; } = state;
    public ulong Code { get; internal set; } = code;
    public string Description { get; internal set; } = description;

    public Error? Error { get; internal set; } = error;
}

public delegate void OutcomeDescriptorCallback(IMessage message, OutcomeDescriptor outcomeDescriptor);

public interface IPublisher : IResourceStatus, IClosable
{
    Task Publish(IMessage message,
        OutcomeDescriptorCallback outcomeCallback); // TODO: Add CancellationToken and callBack
}
