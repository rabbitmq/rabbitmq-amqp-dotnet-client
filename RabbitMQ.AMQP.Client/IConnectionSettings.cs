namespace RabbitMQ.AMQP.Client;

public interface IConnectionSettings : IEquatable<IConnectionSettings>
{
    string Host { get; }
    int Port { get; }
    string VirtualHost { get; }
    string User { get; }
    string Password { get; }
    string Scheme { get; }
    string ConnectionName { get; }
    string Path { get; }
    bool UseSsl { get; }
}
