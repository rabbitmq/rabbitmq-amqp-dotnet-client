using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl;

internal class Visitor(AmqpManagement management) : IVisitor
{
    private AmqpManagement Management { get; } = management;


    public void VisitQueues(List<QueueSpec> queueSpec)
    {
        foreach (var spec in queueSpec)
        {
            Management.Queue(spec).Declare();
        }
    }
}

public class AmqpConnection : IConnection
{
    private Connection? _nativeConnection;
    private AmqpAddress _address = null!;
    private readonly AmqpManagement _management = new();
    private readonly RecordingTopologyListener _recordingTopologyListener = new();

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
            .VirtualHost(address.VirtualHost())
            .ConnectionName(address.ConnectionName())
            .Scheme(address.Scheme())
            .Build();
        _address = amqpAddress;
        await EnsureConnectionAsync();
    }

    internal async Task EnsureConnectionAsync()
    {
        try
        {
            if (_nativeConnection == null || _nativeConnection.IsClosed)
            {
                if (_nativeConnection != null)
                {
                    _nativeConnection.Closed -= MaybeRecoverConnection();
                }

                var open = new Open
                {
                    HostName = $"vhost:{_address.VirtualHost()}",
                    Properties = new Fields()
                    {
                        [new Symbol("connection_name")] = _address.ConnectionName(),
                    }
                };
                _nativeConnection = await Connection.Factory.CreateAsync(_address.Address, open);
                _management.Init(
                    new AmqpManagementParameters(this).TopologyListener(_recordingTopologyListener));

                _nativeConnection.AddClosedCallback(MaybeRecoverConnection());
            }

            Status = Status.Open;
        }
        catch (AmqpException e)
        {
            throw new ConnectionException("AmqpException: Connection failed", e);
        }
        catch (OperationCanceledException e)
        {
            // wrong virtual host
            throw new ConnectionException("OperationCanceledException: Connection failed", e);
        }

        catch (NotSupportedException e)
        {
            // wrong schema
            throw new ConnectionException("NotSupportedException: Connection failed", e);
        }
    }

    private ClosedCallback MaybeRecoverConnection()
    {
        return (sender, error) =>
        {
                var unexpected = Status != Status.Closed;
            Status = Status.Closed;
            Closed?.Invoke(this, unexpected);

            if (unexpected)
            {
                Trace.WriteLine(TraceLevel.Warning, $"connection is closed unexpected" +
                                                    $"{sender} {error} {Status} " +
                                                    $"{_nativeConnection!.IsClosed}");
                var t = Task.Run(async () => { await EnsureConnectionAsync(); });
                t.WaitAsync(TimeSpan.FromSeconds(5));
                _recordingTopologyListener.Accept(new Visitor(_management));
            } else
            {
                Trace.WriteLine(TraceLevel.Verbose, $"connection is closed" +
                                                    $"{sender} {error} {Status} " +
                                                    $"{_nativeConnection!.IsClosed}");
            }


        };
    }

    internal Connection? NativeConnection()
    {
        return _nativeConnection;
    }


    public async Task CloseAsync()
    {
        Status = Status.Closed;
        if (_nativeConnection is { IsClosed: false }) await _nativeConnection.CloseAsync();
        await _management.CloseAsync();
    }

    public event IClosable.ClosedEventHandler? Closed;


    public Status Status { get; private set; } = Status.Closed;
}