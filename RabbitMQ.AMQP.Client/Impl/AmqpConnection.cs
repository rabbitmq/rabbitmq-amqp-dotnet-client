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

/// <summary>
/// AmqpConnection is the concrete implementation of <see cref="IConnection"/>
/// It is a wrapper around the AMQP.Net Lite <see cref="Connection"/> class
/// </summary>
public class AmqpConnection : IConnection
{
    // The native AMQP.Net Lite connection
    private Connection? _nativeConnection;
    private readonly AmqpManagement _management = new();

    private readonly RecordingTopologyListener _recordingTopologyListener = new();

    private readonly ConnectionSettings _connectionSettings;

    /// <summary>
    /// Creates a new instance of <see cref="AmqpConnection"/>
    /// Through the Connection is possible to create:
    ///  - Management. See <see cref="AmqpManagement"/>
    ///  - Publishers and Consumers: TODO: Implement 
    /// </summary>
    /// <param name="connectionSettings"></param>
    /// <returns></returns>
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
            throw new ConnectionException($"AmqpException: Connection failed. Info: {ToString()} ", e);
        }
        catch (OperationCanceledException e)
        {
            // wrong virtual host
            throw new ConnectionException($"OperationCanceledException: Connection failed. Info: {ToString()}", e);
        }

        catch (NotSupportedException e)
        {
            // wrong schema
            throw new ConnectionException($"NotSupportedException: Connection failed. Info: {ToString()}", e);
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
        return async (sender, error) =>
        {
            if (error != null)
            {
                Trace.WriteLine(TraceLevel.Warning, $"connection is closed unexpectedly. " +
                                                    $"Info: {ToString()}");

                if (!_connectionSettings.RecoveryConfiguration.IsActivate())
                {
                    OnNewStatus(Status.Closed, Utils.ConvertError(error));
                    return;
                }
                // TODO: Block the publishers and consumers
                OnNewStatus(Status.Reconneting, Utils.ConvertError(error));
                
                await Task.Run(async () =>
                {
                    var connected = false;
                    while (!connected)
                    {
                        try
                        {
                            var next = _connectionSettings.RecoveryConfiguration.GetBackOffDelayPolicy().Next();
                            Trace.WriteLine(TraceLevel.Information,
                                $"Trying Recovering connection in {next} milliseconds. Info: {ToString()})");
                            await Task.Delay(
                                TimeSpan.FromMilliseconds(next));

                            await EnsureConnectionAsync();
                            connected = true;
                        }
                        catch (Exception e)
                        {
                            Trace.WriteLine(TraceLevel.Warning, $"Error trying to recover connection {e}. Info: {this}");
                        }
                    }
                    
                    _connectionSettings.RecoveryConfiguration.GetBackOffDelayPolicy().Reset();
                    Trace.WriteLine(TraceLevel.Information, $"Connection recovered. Info: {ToString()}");
                    
                    if (_connectionSettings.RecoveryConfiguration.IsTopologyActive())
                    {
                        Trace.WriteLine(TraceLevel.Information, $"Recovering topology. Info: {ToString()}");
                        _recordingTopologyListener.Accept(new Visitor(_management));
                    }
                });
                return;
            }


            Trace.WriteLine(TraceLevel.Verbose, $"connection is closed. Info: {ToString()}");
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
    public override string ToString()
    {
        var info =  $"AmqpConnection{{ConnectionSettings='{_connectionSettings}', Status='{Status.ToString()}'}}";
        return info;
    }
}