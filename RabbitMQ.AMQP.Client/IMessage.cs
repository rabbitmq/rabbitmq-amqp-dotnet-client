namespace RabbitMQ.AMQP.Client;

public interface IMessage
{
    // TODO: Complete the IMessage interface with all the  properties
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


    public IMessage Annotation(string key, object value);

    public object Annotation(string key);
}
