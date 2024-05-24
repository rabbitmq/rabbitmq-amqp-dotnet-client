namespace RabbitMQ.AMQP.Client;

public class AmqpQueueSpecification(AmqpManagement management) : IQueueSpecification
{
    private string? _name;
    private bool? _exclusive;
    private bool? _autoDelete;
    private AmqpManagement _management = management;


    public IQueueSpecification Name(string name)
    {
        _name = name;
        return this;
    }

    public IQueueSpecification Exclusive(bool exclusive)
    {
        _exclusive = exclusive;
        return this;
    }

    public IQueueSpecification AutoDelete(bool autoDelete)
    {
        _autoDelete = autoDelete;
        return this;
    }
}