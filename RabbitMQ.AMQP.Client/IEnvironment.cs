using System.Collections.ObjectModel;

namespace RabbitMQ.AMQP.Client;

public interface IEnvironment
{
    public Task<IConnection> CreateConnectionAsync();

    public ReadOnlyCollection<IConnection> GetConnections();

    Task CloseAsync();
}
