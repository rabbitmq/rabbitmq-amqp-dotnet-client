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

    public async Task ConnectAsync(IAddress address)
    {
        var amqpAddress = new AmqpAddressBuilder()
            .Host(address.Host())
            .Port(address.Port())
            .User(address.User())
            .Password(address.Password())
            .Path(address.Path())
            .Scheme(address.Scheme())
            .Build();
        var connection = await Connection.Factory.CreateAsync(amqpAddress.Address);
        _nativeConnection = connection;
        _management.Init(connection);
    }


    public async Task CloseAsync()
    {
        await _management.CloseAsync();
    }
}