using Amqp;
using Amqp.Framing;

namespace RabbitMQ.AMQP.Client;

public class AmqpConnection : IConnection
{
    private Connection? _nativeConnection;
    
    private readonly AmqpManagement _management = new();
    public IManagement Management()
    {
        return _management;
    }

    public async Task ConnectAsync(AmqpAddress address)
    {
        var amqpAddress = new Amqp.Address(address.Host, address.Port, "guest","guest", "/", "amqp");
        var connection = await Connection.Factory.CreateAsync(amqpAddress);
        _nativeConnection = connection;
        _management.Init(connection);
    }
    
    
    
    public async Task CloseAsync()
    {
        await _management.CloseAsync();
    }
}