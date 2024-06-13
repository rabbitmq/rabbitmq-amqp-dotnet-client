namespace RabbitMQ.AMQP.Client;

public interface IConnectionSettings
{
    string Host();

    int Port();

    string VirtualHost();


    string User();

    string Password();

    string Scheme();

    string ConnectionName();
}