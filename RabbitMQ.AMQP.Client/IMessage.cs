namespace RabbitMQ.AMQP.Client;

public interface IMessage
{
    public object Body();
    // properties
    string MessageId();
    IMessage MessageId(string id);

    string CorrelationId();
    IMessage CorrelationId(string id);

    string ReplyTo();
    IMessage ReplyTo(string id);

    string Subject();
    IMessage Subject(string subject);

}
