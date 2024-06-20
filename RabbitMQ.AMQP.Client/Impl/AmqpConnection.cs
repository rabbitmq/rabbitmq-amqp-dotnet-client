using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl;

internal class Visitor(AmqpManagement management) : IVisitor
{
    private AmqpManagement Management { get; } = management;

    public async Task VisitQueues(List<QueueSpec> queueSpec)
    {
        foreach (var spec in queueSpec)
        {
            Trace.WriteLine(TraceLevel.Information, $"Recovering queue {spec.Name}");
            await Management.Queue(spec).Declare();
        }
    }
}

/// <summary>
/// AmqpConnection is the concrete implementation of <see cref="IConnection"/>
/// It is a wrapper around the AMQP.Net Lite <see cref="Connection"/> class
/// </summary>
public class AmqpConnection : IConnection
{
    private const string ConnectionNotRecoveredCode = "CONNECTION_NOT_RECOVERED";
    private const string ConnectionNotRecoveredMessage = "Connection not recovered";

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

            OnNewStatus(State.Open, null);
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


    private void OnNewStatus(State newState, Error? error)
    {
        if (State == newState) return;
        var oldStatus = State;
        State = newState;
        ChangeState?.Invoke(this, oldStatus, newState, error);
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
                    OnNewStatus(State.Closed, Utils.ConvertError(error));
                    return;
                }

                // TODO: Block the publishers and consumers
                OnNewStatus(State.Reconnecting, Utils.ConvertError(error));

                await Task.Run(async () =>
                {
                    var connected = false;
                    // as first step we try to recover the connection
                    // so the connected flag is false
                    while (!connected &&
                           // we have to check if the recovery is active.
                           // The user may want to disable the recovery mechanism
                           // the user can use the lifecycle callback to handle the error
                           _connectionSettings.RecoveryConfiguration.IsActivate() &&
                           // we have to check if the backoff policy is active
                           // the user may want to disable the backoff policy or 
                           // the backoff policy is not active due of some condition
                           // for example: Reaching the maximum number of retries and avoid the forever loop
                           _connectionSettings.RecoveryConfiguration.GetBackOffDelayPolicy().IsActive)
                    {
                        try
                        {
                            var next = _connectionSettings.RecoveryConfiguration.GetBackOffDelayPolicy().Delay();
                            Trace.WriteLine(TraceLevel.Information,
                                $"Trying Recovering connection in {next} milliseconds. Info: {ToString()})");
                            await Task.Delay(
                                TimeSpan.FromMilliseconds(next));

                            await EnsureConnectionAsync();
                            connected = true;
                        }
                        catch (Exception e)
                        {
                            Trace.WriteLine(TraceLevel.Warning,
                                $"Error trying to recover connection {e}. Info: {this}");
                        }
                    }

                    _connectionSettings.RecoveryConfiguration.GetBackOffDelayPolicy().Reset();
                    var connectionDescription = connected ? "recovered" : "not recovered";
                    Trace.WriteLine(TraceLevel.Information,
                        $"Connection {connectionDescription}. Info: {ToString()}");

                    if (!connected)
                    {
                        Trace.WriteLine(TraceLevel.Verbose, $"connection is closed. Info: {ToString()}");
                        OnNewStatus(State.Closed, new Error()
                        {
                            Description =
                                $"{ConnectionNotRecoveredMessage}, recover status: {_connectionSettings.RecoveryConfiguration}",
                            ErrorCode = ConnectionNotRecoveredCode
                        });
                        return;
                    }


                    if (_connectionSettings.RecoveryConfiguration.IsTopologyActive())
                    {
                        Trace.WriteLine(TraceLevel.Information, $"Recovering topology. Info: {ToString()}");
                        await _recordingTopologyListener.Accept(new Visitor(_management));
                    }
                });
                return;
            }


            Trace.WriteLine(TraceLevel.Verbose, $"connection is closed. Info: {ToString()}");
            OnNewStatus(State.Closed, Utils.ConvertError(error));
        };
    }

    internal Connection? NativeConnection()
    {
        return _nativeConnection;
    }


    public async Task CloseAsync()
    {
        _recordingTopologyListener.Clear();
        if (State == State.Closed) return;
        OnNewStatus(State.Closing, null);
        if (_nativeConnection is { IsClosed: false }) await _nativeConnection.CloseAsync();
        await _management.CloseAsync();
    }

    // TODO: maybe add a listener like java client
    public event IClosable.LifeCycleCallBack? ChangeState;

    public State State { get; private set; } = State.Closed;

    public override string ToString()
    {
        var info = $"AmqpConnection{{ConnectionSettings='{_connectionSettings}', Status='{State.ToString()}'}}";
        return info;
    }
}