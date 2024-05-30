namespace RabbitMQ.AMQP.Client;

public interface IAddress
{
    string Host();

    int Port();

    string Path();

    string User();

    string Password();

    string Scheme();
}