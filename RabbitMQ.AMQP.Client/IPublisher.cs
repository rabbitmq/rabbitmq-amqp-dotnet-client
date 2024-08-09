namespace RabbitMQ.AMQP.Client;

public class PublisherException(string message) : Exception(message);

public enum OutcomeState
{
    Accepted,
    Failed,
}

public class PublishOutcome
{
    private readonly OutcomeState _state;
    private readonly Error? _error;

    public PublishOutcome(OutcomeState state, Error? error)
    {
        _state = state;
        _error = error;
    }

    public OutcomeState State => _state;
    public Error? Error => _error;
}

public class PublishResult
{
    private IMessage _message;
    private PublishOutcome _outcome;

    public PublishResult(IMessage message, PublishOutcome outcome)
    {
        _message = message;
        _outcome = outcome;
    }

    public IMessage Message => _message;
    public PublishOutcome Outcome => _outcome;
}

public interface IPublisher : ILifeCycle
{
    Task<PublishResult> PublishAsync(IMessage message, CancellationToken cancellationToken = default);
}
