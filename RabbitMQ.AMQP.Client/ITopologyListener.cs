namespace RabbitMQ.AMQP.Client;

internal interface ITopologyListener
{
    void QueueDeclared(IQueueSpecification specification);

    void QueueDeleted(string name);

    void ExchangeDeclared(IExchangeSpecification specification);

    void ExchangeDeleted(string name);

    void BindingDeclared(IBindingSpecification specification);

    void BindingDeleted(string path);

    void Clear();

    int QueueCount();

    int ExchangeCount();

    int BindingCount();
}
