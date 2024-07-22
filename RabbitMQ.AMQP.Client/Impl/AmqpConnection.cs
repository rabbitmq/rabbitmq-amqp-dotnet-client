using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl;

internal class Visitor(AmqpManagement management) : IVisitor
{
    private AmqpManagement Management { get; } = management;

    public async Task VisitQueues(IEnumerable<QueueSpec> queueSpec)
    {
        foreach (QueueSpec spec in queueSpec)
        {
            Trace.WriteLine(TraceLevel.Information, $"Recovering queue {spec.Name}");
            try
            {
                await Management.Queue(spec).Declare()
                    .ConfigureAwait(false);
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
public class AmqpConnection : AbstractResourceStatus, IConnection
{
    private const string ConnectionNotRecoveredCode = "CONNECTION_NOT_RECOVERED";
    private const string ConnectionNotRecoveredMessage = "Connection not recovered";
    private readonly SemaphoreSlim _semaphoreClose = new(1, 1);


    // The native AMQP.Net Lite connection
    private Connection? _nativeConnection;

    private readonly AmqpManagement _management = new();
    private readonly RecordingTopologyListener _recordingTopologyListener = new();

    private readonly TaskCompletionSource<bool> _connectionCloseTaskCompletionSource =
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private readonly ConnectionSettings _connectionSettings;
    internal readonly AmqpSessionManagement NativePubSubSessions;

    // TODO: Implement the semaphore to avoid multiple connections
    // private readonly SemaphoreSlim _semaphore = new(1, 1);


    /// <summary>
    /// Publishers contains all the publishers created by the connection.
    /// Each connection can have multiple publishers.
    /// They key is the publisher Id ( a Guid)  
    /// See <see cref="AmqpPublisher"/>
    /// </summary>
    internal ConcurrentDictionary<string, IPublisher> Publishers { get; } = new();

    internal ConcurrentDictionary<string, IConsumer> Consumers { get; } = new();


    public ReadOnlyCollection<IPublisher> GetPublishers()
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
    public static async Task<IConnection> CreateAsync(ConnectionSettings connectionSettings)
    {
        var connection = new AmqpConnection(connectionSettings);
        await connection.ConnectAsync()
            .ConfigureAwait(false);
        return connection;
    }

    private void PauseAllPublishers()
    {
        foreach (AmqpPublisher publisher in Publishers.Values)
        {
            publisher.PausePublishing();
        }
    }

    private void ResumeAllPublishers()
    {
        foreach (AmqpPublisher publisher in Publishers.Values)
        {
            publisher.ResumePublishing();
        }
    }


    /// <summary>
    /// Closes all the publishers. It is called when the connection is closed.
    /// </summary>
    private async Task CloseAllPublishers()
    {
        var cloned = new List<IPublisher>(Publishers.Values);

        foreach (IPublisher publisher in cloned)
        {
            await publisher.CloseAsync()
                .ConfigureAwait(false);
        }
    }

    private async Task CloseAllConsumers()
    {
        var cloned = new List<IConsumer>(Consumers.Values);

        foreach (IConsumer consumer in cloned)
        {
            await consumer.CloseAsync()
                .ConfigureAwait(false);
        }
    }

    private AmqpConnection(ConnectionSettings connectionSettings)
    {
        _connectionSettings = connectionSettings;
        NativePubSubSessions = new AmqpSessionManagement(this, 1);
    }

    public IManagement Management()
    {
        return _management;
    }

    public IConsumerBuilder ConsumerBuilder()
    {
        return new AmqpConsumerBuilder(this);
    }

    private Task ConnectAsync()
    {
        EnsureConnection();
        OnNewStatus(State.Open, null);
        return Task.CompletedTask;
    }

    private void EnsureConnection()
    {
        // await _semaphore.WaitAsync();
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

                var manualReset = new ManualResetEvent(false);
                // TODO ConnectionFactory.CreateAsync
                _nativeConnection = new Connection(_connectionSettings.Address, null, open, (connection, open1) =>
                {
                    manualReset.Set();
                    Trace.WriteLine(TraceLevel.Information, $"Connection opened. Info: {ToString()}");
                    OnNewStatus(State.Open, null);
                });

                manualReset.WaitOne(TimeSpan.FromSeconds(5));
                if (_nativeConnection.IsClosed)
                {
                    throw new ConnectionException(
                        $"Connection failed. Info: {ToString()}, error: {_nativeConnection.Error}");
                }

                _management.Init(
                    new AmqpManagementParameters(this).TopologyListener(_recordingTopologyListener));

                _nativeConnection.Closed += MaybeRecoverConnection();
            }
        }

        catch (AmqpException e)
        {
            Trace.WriteLine(TraceLevel.Error, $"Error trying to connect. Info: {ToString()}, error: {e}");
            throw new ConnectionException($"Error trying to connect. Info: {ToString()}, error: {e}");
        }


        finally
        {
            // _semaphore.Release();
        }

        // return Task.CompletedTask;
    }

    /// <summary>
    /// Event handler for the connection closed event.
    /// In case the error is null means that the connection is closed by the user.
    /// The recover mechanism is activated only if the error is not null.
    /// The connection maybe recovered if the recovery configuration is active.
    /// </summary>
    /// <returns></returns>
    private ClosedCallback MaybeRecoverConnection()
    {
        return async (sender, error) =>
        {
            await _semaphoreClose.WaitAsync().ConfigureAwait(false);

            try
            {
                if (error != null)
                {
                    Trace.WriteLine(TraceLevel.Warning, $"connection is closed unexpectedly. " +
                                                        $"Info: {ToString()}");

                    // we have to check if the recovery is active.
                    // The user may want to disable the recovery mechanism
                    // the user can use the lifecycle callback to handle the error
                    if (!_connectionSettings.RecoveryConfiguration.IsActivate())
                    {
                        OnNewStatus(State.Closed, Utils.ConvertError(error));
                        return;
                    }

                    // TODO: Block the publishers and consumers
                    OnNewStatus(State.Reconnecting, Utils.ConvertError(error));
                    await Task.Run(async () =>
                    {
                        bool connected = false;
                        // as first step we try to recover the connection
                        // so the connected flag is false
                        while (!connected &&
                               // we have to check if the backoff policy is active
                               // the user may want to disable the backoff policy or 
                               // the backoff policy is not active due of some condition
                               // for example: Reaching the maximum number of retries and avoid the forever loop
                               _connectionSettings.RecoveryConfiguration.GetBackOffDelayPolicy().IsActive() &&

                               // even we set the status to reconnecting up, we need to check if the connection is still in the
                               // reconnecting status. The user may close the connection in the meanwhile
                               State == State.Reconnecting)
                        {
                            try
                            {
                                int next = _connectionSettings.RecoveryConfiguration.GetBackOffDelayPolicy().Delay();
                                Trace.WriteLine(TraceLevel.Information,
                                    $"Trying Recovering connection in {next} milliseconds. Info: {ToString()})");
                                await Task.Delay(TimeSpan.FromMilliseconds(next))
                                    .ConfigureAwait(false);

                                EnsureConnection();
                                connected = true;
                            }
                            catch (Exception e)
                            {
                                Trace.WriteLine(TraceLevel.Warning,
                                    $"Error trying to recover connection {e}. Info: {this}");
                            }
                        }

                        _connectionSettings.RecoveryConfiguration.GetBackOffDelayPolicy().Reset();
                        string connectionDescription = connected ? "recovered" : "not recovered";
                        Trace.WriteLine(TraceLevel.Information,
                            $"Connection {connectionDescription}. Info: {ToString()}");

                        if (!connected)
                        {
                            Trace.WriteLine(TraceLevel.Verbose, $"connection is closed. Info: {ToString()}");
                            OnNewStatus(State.Closed,
                                new Error(ConnectionNotRecoveredCode,
                                    $"{ConnectionNotRecoveredMessage}, recover status: {_connectionSettings.RecoveryConfiguration}"));
                            return;
                        }

                        if (_connectionSettings.RecoveryConfiguration.IsTopologyActive())
                        {
                            Trace.WriteLine(TraceLevel.Information, $"Recovering topology. Info: {ToString()}");
                            var visitor = new Visitor(_management);
                            await _recordingTopologyListener.Accept(visitor)
                                .ConfigureAwait(false);
                        }

                        OnNewStatus(State.Open, null);
                    }).ConfigureAwait(false);


                    return;
                }


                Trace.WriteLine(TraceLevel.Verbose, $"connection is closed. Info: {ToString()}");
                OnNewStatus(State.Closed, Utils.ConvertError(error));
            }
            finally
            {
                _semaphoreClose.Release();
            }

            _connectionCloseTaskCompletionSource.SetResult(true);
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


    public async Task CloseAsync()
    {
        await _semaphoreClose.WaitAsync()
            .ConfigureAwait(false);
        try
        {
            await CloseAllPublishers().ConfigureAwait(false);
            await CloseAllConsumers().ConfigureAwait(false);

            _recordingTopologyListener.Clear();

            if (State == State.Closed)
            {
                return;
            }

            OnNewStatus(State.Closing, null);

            await _management.CloseAsync()
                .ConfigureAwait(false);
            
            if (_nativeConnection is { IsClosed: false })
            {
                await _nativeConnection.CloseAsync()
                    .ConfigureAwait(false);
            }

          
        }
        finally
        {
            _semaphoreClose.Release();
        }

        await _connectionCloseTaskCompletionSource.Task.WaitAsync(TimeSpan.FromSeconds(10))
            .ConfigureAwait(false);

        OnNewStatus(State.Closed, null);
    }


    public override string ToString()
    {
        var info = $"AmqpConnection{{ConnectionSettings='{_connectionSettings}', Status='{State.ToString()}'}}";
        return info;
    }
}
