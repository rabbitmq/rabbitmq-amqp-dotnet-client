namespace RabbitMQ.AMQP.Client;

public interface IAddress
{
    string Host();

    int Port();

    string VirtualHost();
    

    string User();

    string Password();

    string Scheme();

    string ConnectionName();
}