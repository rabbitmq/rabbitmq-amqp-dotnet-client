using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client;

public class AmqpConnection : IConnection, IResource
{
    private Connection? _nativeConnection;
    private AmqpAddress _address = null!;
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
            .ConnectionName(address.ConnectionName())
            .Scheme(address.Scheme())
            .Build();
        _address = amqpAddress;
        await EnsureConnectionAsync();
    }

    internal async Task EnsureConnectionAsync()
    {
        if (_nativeConnection == null || _nativeConnection.IsClosed)
        {
            var open = new Open
            {
                Properties = new Fields()
                {
                    [new Symbol("connection_name")] = _address.ConnectionName()
                }
            };
            var connection = await Connection.Factory.CreateAsync(_address.Address, open);
            connection.Closed += (sender, error) =>
            {
                var unexpected = Status != Status.Closed;
                Status = Status.Closed;

                Closed?.Invoke(this, unexpected);

                Trace.WriteLine(TraceLevel.Warning, $"connection is closed " +
                                                    $"{sender} {error} {Status} " +
                                                    $"{connection.IsClosed}");
            };
            _nativeConnection = connection;
            _management.Init(connection);
        }

        Status = Status.Open;
    }


    public async Task CloseAsync()
    {
        Status = Status.Closed;
        if (_nativeConnection is { IsClosed: false }) await _nativeConnection.CloseAsync();
        await _management.CloseAsync();
    }

    public event IResource.ClosedEventHandler? Closed;


    public Status Status { get; private set; } = Status.Closed;
}