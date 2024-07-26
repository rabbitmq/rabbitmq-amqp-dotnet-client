using System.Collections.ObjectModel;

namespace RabbitMQ.AMQP.Client;


/// <summary>
/// Interface to create IConnections and manage them.
/// </summary>
public interface IEnvironment
{
    /// <summary>
    /// Create a new connection with the given connection settings.
    /// </summary>
    /// <param name="connectionSettings"></param>
    /// <returns>IConnection</returns>
    public Task<IConnection> CreateConnectionAsync(IConnectionSettings connectionSettings);
    
    
    /// <summary>
    /// Create a new connection with the default connection settings.
    /// </summary>
    /// <returns>IConnection</returns>
    
    public Task<IConnection> CreateConnectionAsync();


    public ReadOnlyCollection<IConnection> GetConnections();

    /// <summary>
    /// Close all connections.
    /// </summary>
    /// <returns></returns>
    
    Task CloseAsync();
}
