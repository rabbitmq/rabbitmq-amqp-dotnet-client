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
            Trace.WriteLine(TraceLevel.Information, $"Recovering queue {spec.Name}");
            Management.Queue(spec).Declare();
        }
    }
}

public class AmqpConnection : IConnection
{
    private Connection? _nativeConnection;
    private readonly AmqpManagement _management = new();
    private readonly RecordingTopologyListener _recordingTopologyListener = new();
    private readonly ConnectionSettings _connectionSettings;

    public static async Task<AmqpConnection> CreateAsync(ConnectionSettings connectionSettings)
    {
        var connection = new AmqpConnection(connectionSettings);
        await connection.EnsureConnectionAsync();
        return connection;
    }

    private AmqpConnection(ConnectionSettings connectionSettings)
    {
        _connectionSettings = connectionSettings;
    }


    public IManagement Management()
    {
        return _management;
    }

    public async Task ConnectAsync()
    {
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
                    HostName = $"vhost:{_connectionSettings.VirtualHost()}",
                    Properties = new Fields()
                    {
                        [new Symbol("connection_name")] = _connectionSettings.ConnectionName(),
                    }
                };
                _nativeConnection = await Connection.Factory.CreateAsync(_connectionSettings.Address, open);
                _management.Init(
                    new AmqpManagementParameters(this).TopologyListener(_recordingTopologyListener));

                _nativeConnection.AddClosedCallback(MaybeRecoverConnection());
            }

            OnNewStatus(Status.Open, null);
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

    private void OnNewStatus(Status newStatus, Error? error)
    {
        if (Status == newStatus) return;
        var oldStatus = Status;
        Status = newStatus;
        ChangeStatus?.Invoke(this, oldStatus, newStatus, error);
    }

    private ClosedCallback MaybeRecoverConnection()
    {
        return (sender, error) =>
        {
            if (error != null)
            {
                // TODO: Implement Dump Interface
                Trace.WriteLine(TraceLevel.Warning, $"connection is closed unexpected" +
                                                    $"{sender} {error} {Status} " +
                                                    $"{_nativeConnection!.IsClosed}");

                if (!_connectionSettings.RecoveryConfiguration.IsActivate())
                {
                    OnNewStatus(Status.Closed, Utils.ConvertError(error));
                    return;
                }


                OnNewStatus(Status.Reconneting, Utils.ConvertError(error));

                Thread.Sleep(1000);
                // TODO: Replace with Backoff pattern
                var t = Task.Run(async () =>
                {
                    Trace.WriteLine(TraceLevel.Information, "Recovering connection");
                    await EnsureConnectionAsync();
                    Trace.WriteLine(TraceLevel.Information, "Recovering topology");
                    if (_connectionSettings.RecoveryConfiguration.IsTopologyActive())
                    {
                        _recordingTopologyListener.Accept(new Visitor(_management));
                    }
                });
                t.WaitAsync(TimeSpan.FromSeconds(10));
                return;
            }


            Trace.WriteLine(TraceLevel.Verbose, $"connection is closed" +
                                                $"{sender} {error} {Status} " +
                                                $"{_nativeConnection!.IsClosed}");
            OnNewStatus(Status.Closed, Utils.ConvertError(error));
        };
    }

    internal Connection? NativeConnection()
    {
        return _nativeConnection;
    }


    public async Task CloseAsync()
    {
        OnNewStatus(Status.Closed, null);
        if (_nativeConnection is { IsClosed: false }) await _nativeConnection.CloseAsync();
        await _management.CloseAsync();
    }

    public event IClosable.ChangeStatusCallBack? ChangeStatus;

    public Status Status { get; private set; } = Status.Closed;
}