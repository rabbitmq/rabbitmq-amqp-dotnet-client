using System.Collections.Concurrent;
using System.Collections.ObjectModel;
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
            try
            {
                await Management.Queue(spec).Declare();
            }
            catch (Exception e)
            {
                Trace.WriteLine(TraceLevel.Error,
                    $"Error recovering queue {spec.Name}. Error: {e}. Management Status: {Management}");
            }
        }
    }
}

/// <summary>
/// AmqpConnection is the concrete implementation of <see cref="IConnection"/>
/// It is a wrapper around the AMQP.Net Lite <see cref="Connection"/> class
/// </summary>
public class AmqpConnection : AbstractClosable, IConnection
{
    private const string ConnectionNotRecoveredCode = "CONNECTION_NOT_RECOVERED";
    private const string ConnectionNotRecoveredMessage = "Connection not recovered";

    // The native AMQP.Net Lite connection
    private Connection? _nativeConnection;
    private Session? _nativeSession;
    private readonly AmqpManagement _management = new();

    private readonly RecordingTopologyListener _recordingTopologyListener = new();
    private readonly ConnectionSettings _connectionSettings;
    private readonly SemaphoreSlim _semaphore = new(1, 1);

    internal ConcurrentDictionary<string, AmqpPublisher> Publishers { get; } = new();

    public ReadOnlyCollection<AmqpPublisher> GetPublishers()
    {
        return Publishers.Values.ToList().AsReadOnly();
    }

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
        await connection.ConnectAsync();
        return connection;
    }

    private void PauseAllPublishers()
    {
        foreach (var publisher in Publishers.Values)
        {
            publisher.PausePublishing();
        }
    }

    private void ResumeAllPublishers()
    {
        foreach (var publisher in Publishers.Values)
        {
            publisher.ResumePublishing();
        }
    }

    private async Task CloseAllPublishers()
    {
        var cloned = new List<AmqpPublisher>(Publishers.Values);

        foreach (var publisher in cloned)
        {
            await publisher.CloseAsync();
        }
    }

    private AmqpConnection(ConnectionSettings connectionSettings)
    {
        _connectionSettings = connectionSettings;
    }

    internal Session GetNativeSession()
    {
        if (_nativeSession == null || _nativeSession.IsClosed)
        {
            _nativeSession = new Session(_nativeConnection);
        }

        return _nativeSession;
    }


    public IManagement Management()
    {
        return _management;
    }

    public async Task ConnectAsync()
    {
        await EnsureConnectionAsync();
        OnNewStatus(State.Open, null);
    }

    private async Task EnsureConnectionAsync()
    {
        await _semaphore.WaitAsync();
        try
        {
            if (_nativeConnection == null || _nativeConnection.IsClosed)
            {
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

                _nativeConnection.Closed += MaybeRecoverConnection();
            }
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
        finally
        {
            _semaphore.Release();
        }
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

                    OnNewStatus(State.Open, null);
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


    public IPublisherBuilder PublisherBuilder()
    {
        ThrowIfClosed();
        var publisherBuilder = new AmqpPublisherBuilder(this);
        return publisherBuilder;
    }


    public override async Task CloseAsync()
    {
        await CloseAllPublishers();
        _recordingTopologyListener.Clear();
        if (State == State.Closed) return;
        OnNewStatus(State.Closing, null);
        if (_nativeConnection is { IsClosed: false }) await _nativeConnection.CloseAsync();
        await _management.CloseAsync();
    }


    public override string ToString()
    {
        var info = $"AmqpConnection{{ConnectionSettings='{_connectionSettings}', Status='{State.ToString()}'}}";
        return info;
    }
}