// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.Collections.Concurrent;
using System.Collections.ObjectModel;
using Amqp;
using Amqp.Framing;
using Amqp.Sasl;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl;

/// <summary>
/// AmqpConnection is the concrete implementation of <see cref="IConnection"/>
/// It is a wrapper around the AMQP.Net Lite <see cref="Connection"/> class
/// </summary>
public class AmqpConnection : AbstractLifeCycle, IConnection
{
    private const string ConnectionNotRecoveredCode = "CONNECTION_NOT_RECOVERED";
    private const string ConnectionNotRecoveredMessage = "Connection not recovered";

    private readonly SemaphoreSlim _semaphoreClose = new(1, 1);
    private readonly SemaphoreSlim _semaphoreOpen = new(1, 1);

    // The native AMQP.Net Lite connection
    private Connection? _nativeConnection;
    private ClosedCallback? _closedCallback;

    private readonly AmqpManagement _management;
    private readonly RecordingTopologyListener _recordingTopologyListener = new();

    private readonly IConnectionSettings _connectionSettings;
    internal readonly AmqpSessionManagement _nativePubSubSessions;

    /// <summary>
    /// Publishers contains all the publishers created by the connection.
    /// Each connection can have multiple publishers.
    /// They key is the publisher Id ( a Guid)  
    /// See <see cref="AmqpPublisher"/>
    /// </summary>
    internal ConcurrentDictionary<Guid, IPublisher> Publishers { get; } = new();
    internal ConcurrentDictionary<Guid, IConsumer> Consumers { get; } = new();

    public ReadOnlyCollection<IPublisher> GetPublishers()
    {
        return Publishers.Values.ToList().AsReadOnly();
    }

    public ReadOnlyCollection<IConsumer> GetConsumers()
    {
        return Consumers.Values.ToList().AsReadOnly();
    }

    public long Id { get; set; }
    /// <summary>
    /// Creates a new instance of <see cref="AmqpConnection"/>
    /// Through the Connection is possible to create:
    ///  - Management. See <see cref="AmqpManagement"/>
    ///  - Publishers and Consumers: See <see cref="AmqpPublisherBuilder"/> and <see cref="AmqpConsumerBuilder"/> 
    /// </summary>
    /// <param name="connectionSettings"></param>
    /// <returns></returns>
    public static async Task<IConnection> CreateAsync(IConnectionSettings connectionSettings)
    {
        var connection = new AmqpConnection(connectionSettings);
        await connection.OpenAsync()
            .ConfigureAwait(false);
        return connection;
    }

    public IManagement Management()
    {
        return _management;
    }

    public IConsumerBuilder ConsumerBuilder()
    {
        return new AmqpConsumerBuilder(this);
    }

    // TODO cancellation token
    public override async Task OpenAsync()
    {
        // TODO cancellation token
        await OpenConnectionAsync(CancellationToken.None)
            .ConfigureAwait(false);
        await base.OpenAsync()
            .ConfigureAwait(false);
    }

    public IPublisherBuilder PublisherBuilder()
    {
        ThrowIfClosed();
        var publisherBuilder = new AmqpPublisherBuilder(this);
        return publisherBuilder;
    }

    public override async Task CloseAsync()
    {
        await _semaphoreClose.WaitAsync()
            .ConfigureAwait(false);
        try
        {
            await CloseAllPublishersAsync()
                .ConfigureAwait(false);
            await CloseAllConsumersAsync()
                .ConfigureAwait(false);

            _recordingTopologyListener.Clear();
            _nativePubSubSessions.ClearSessions();

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
        string info = $"AmqpConnection{{ConnectionSettings='{_connectionSettings}', Status='{State.ToString()}'}}";
        return info;
    }

    internal Connection? NativeConnection
    {
        get
        {
            return _nativeConnection;
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            if (_nativeConnection is not null &&
                _closedCallback is not null)
            {
                _nativeConnection.Closed -= _closedCallback;
            }
            _semaphoreOpen.Dispose();
            _semaphoreClose.Dispose();
        }

        base.Dispose(disposing);
    }

    /// <summary>
    /// Closes all the publishers. It is called when the connection is closed.
    /// </summary>
    // TODO cancellation token, parallel?
    private async Task CloseAllPublishersAsync()
    {
        var cloned = new List<IPublisher>(Publishers.Values);
        foreach (IPublisher publisher in cloned)
        {
            await publisher.CloseAsync()
                .ConfigureAwait(false);
        }
    }

    // TODO cancellation token, parallel?
    private async Task CloseAllConsumersAsync()
    {
        var cloned = new List<IConsumer>(Consumers.Values);
        foreach (IConsumer consumer in cloned)
        {
            await consumer.CloseAsync()
                .ConfigureAwait(false);
        }
    }

    private AmqpConnection(IConnectionSettings connectionSettings)
    {
        _connectionSettings = connectionSettings;
        _nativePubSubSessions = new AmqpSessionManagement(this, 1);
        _management =
            new AmqpManagement(new AmqpManagementParameters(this).TopologyListener(_recordingTopologyListener));
    }

    private async Task OpenConnectionAsync(CancellationToken cancellationToken)
    {
        await _semaphoreOpen.WaitAsync()
            .ConfigureAwait(false);
        try
        {
            if (_nativeConnection is not null &&
                _nativeConnection.ConnectionState == ConnectionState.Opened)
            {
                return;
            }

            var open = new Open
            {
                // Note: no need to set cf.AMQP.HostName
                HostName = $"vhost:{_connectionSettings.VirtualHost}",
                // Note: no need to set cf.AMQP.ContainerId
                ContainerId = _connectionSettings.ContainerId,
                Properties = new Fields()
                {
                    [new Symbol("connection_name")] = _connectionSettings.ContainerId,
                }
            };

            if (_connectionSettings.MaxFrameSize > uint.MinValue)
            {
                // Note: when set here, there is no need to set cf.AMQP.MaxFrameSize
                open.MaxFrameSize = _connectionSettings.MaxFrameSize;
            }

            var cf = new ConnectionFactory();

            if (_connectionSettings is { UseSsl: true, TlsSettings: not null })
            {
                cf.SSL.Protocols = _connectionSettings.TlsSettings.Protocols;
                cf.SSL.CheckCertificateRevocation = _connectionSettings.TlsSettings.CheckCertificateRevocation;

                if (_connectionSettings.TlsSettings.ClientCertificates.Count > 0)
                {
                    cf.SSL.ClientCertificates = _connectionSettings.TlsSettings.ClientCertificates;
                }

                if (_connectionSettings.TlsSettings.LocalCertificateSelectionCallback is not null)
                {
                    cf.SSL.LocalCertificateSelectionCallback =
                        _connectionSettings.TlsSettings.LocalCertificateSelectionCallback;
                }

                if (_connectionSettings.TlsSettings.RemoteCertificateValidationCallback is not null)
                {
                    cf.SSL.RemoteCertificateValidationCallback =
                        _connectionSettings.TlsSettings.RemoteCertificateValidationCallback;
                }
            }

            if (_connectionSettings.SaslMechanism == SaslMechanism.External)
            {
                cf.SASL.Profile = SaslProfile.External;
            }

            void OnOpened(Amqp.IConnection connection, Open open1)
            {
                Trace.WriteLine(TraceLevel.Verbose, $"{ToString()} is open");
                OnNewStatus(State.Open, null);
            }

            try
            {
                ConnectionSettings connectionSettings;
                if (_connectionSettings is null)
                {
                    // TODO create "internal bug" exception type?
                    throw new InvalidOperationException("_connectionSettings is null, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
                }
                else
                {
                    connectionSettings = (ConnectionSettings)_connectionSettings;
                    Address address = connectionSettings.Address;
                    _nativeConnection = await cf.CreateAsync(address: address, open: open, onOpened: OnOpened)
                        .ConfigureAwait(false);
                }
            }
            catch (Exception ex)
            {
                throw new ConnectionException(
                    $"{ToString()} connection failed.", ex);
            }

            if (_nativeConnection.IsClosed)
            {
                throw new ConnectionException(
                    $"{ToString()} connection failed., error: {_nativeConnection.Error}");
            }

            await _management.OpenAsync()
                .ConfigureAwait(false);

            _closedCallback = BuildClosedCallback();
            _nativeConnection.AddClosedCallback(_closedCallback);
        }
        catch (AmqpException e)
        {
            Trace.WriteLine(TraceLevel.Error, $"{ToString()} - Error trying to connect, error: {e}");
            throw new ConnectionException($"{ToString()} - Error trying to connect., error: {e}");
        }
        finally
        {
            _semaphoreOpen.Release();
        }
    }

    /// <summary>
    /// Event handler for the connection closed event.
    /// In case the error is null means that the connection is closed by the user.
    /// The recover mechanism is activated only if the error is not null.
    /// The connection maybe recovered if the recovery configuration is active.
    ///
    /// TODO this method could be improved.
    /// MaybeRecoverConnection should set a connection state to RECOVERING
    /// and then kick off a task dedicated to recovery
    /// </summary>
    /// <returns></returns>
    private ClosedCallback BuildClosedCallback()
    {
        return async (sender, error) =>
        {
            if (_disposed)
            {
                return;
            }

            await _semaphoreClose.WaitAsync()
                .ConfigureAwait(false);
            try
            {
                // close all the sessions, if the connection is closed the sessions are not valid anymore
                _nativePubSubSessions.ClearSessions();

                void DoClose(Error? argError = null)
                {
                    Error? err = argError ?? Utils.ConvertError(error);
                    Trace.WriteLine(TraceLevel.Verbose, $"{ToString()} is closed");
                    OnNewStatus(State.Closed, err);
                    ChangeEntitiesStatus(State.Closed, err);
                    _connectionCloseTaskCompletionSource.SetResult(true);
                }

                if (error is null)
                {
                    DoClose();
                    return;
                }
                else
                {
                    //  we assume here that the connection is closed unexpectedly, since the error is not null
                    Trace.WriteLine(TraceLevel.Warning, $"{ToString()} closed unexpectedly.");

                    // we have to check if the recovery is active.
                    // The user may want to disable the recovery mechanism
                    // the user can use the lifecycle callback to handle the error
                    if (false == _connectionSettings.Recovery.IsActivate())
                    {
                        DoClose();
                        return;
                    }

                    // change the status for the connection and all the entities
                    // to reconnecting and all the events are fired
                    OnNewStatus(State.Reconnecting, Utils.ConvertError(error));
                    ChangeEntitiesStatus(State.Reconnecting, Utils.ConvertError(error));

                    IBackOffDelayPolicy backOffDelayPolicy = _connectionSettings.Recovery.GetBackOffDelayPolicy();
                    bool connected = false;
                    // as first step we try to recover the connection
                    // so the connected flag is false
                    while (false == connected &&
                           // we have to check if the backoff policy is active
                           // the user may want to disable the backoff policy or 
                           // the backoff policy is not active due of some condition
                           // for example: Reaching the maximum number of retries and avoid the forever loop
                           backOffDelayPolicy.IsActive() &&

                           // even we set the status to reconnecting up, we need to check if the connection is still in the
                           // reconnecting status. The user may close the connection in the meanwhile
                           State == State.Reconnecting)
                    {
                        try
                        {
                            int nextDelayMs = backOffDelayPolicy.Delay();

                            Trace.WriteLine(TraceLevel.Information,
                                $"{ToString()} is trying Recovering connection in {nextDelayMs} milliseconds, " +
                                $"attempt: {_connectionSettings.Recovery.GetBackOffDelayPolicy().CurrentAttempt}. ");

                            await Task.Delay(TimeSpan.FromMilliseconds(nextDelayMs))
                                .ConfigureAwait(false);

                            // TODO cancellation token
                            await OpenConnectionAsync(CancellationToken.None)
                                .ConfigureAwait(false);

                            connected = true;
                        }
                        catch (Exception e)
                        {
                            // TODO this could / should be more visible to the user, perhaps?
                            Trace.WriteLine(TraceLevel.Warning,
                                $"{ToString()} Error trying to recover connection {e}");
                        }
                    }

                    backOffDelayPolicy.Reset();
                    string connectionDescription = connected ? "recovered" : "not recovered";
                    Trace.WriteLine(TraceLevel.Information,
                        $"{ToString()} is {connectionDescription}");

                    if (false == connected)
                    {
                        var notRecoveredError = new Error(ConnectionNotRecoveredCode,
                                $"{ConnectionNotRecoveredMessage}," +
                                $"recover status: {_connectionSettings.Recovery}");
                        DoClose(notRecoveredError);
                        return;
                    }

                    if (_connectionSettings.Recovery.IsTopologyActive())
                    {
                        Trace.WriteLine(TraceLevel.Information, $"{ToString()} Recovering topology");
                        var visitor = new Visitor(_management);
                        await _recordingTopologyListener.Accept(visitor)
                            .ConfigureAwait(false);
                    }

                    OnNewStatus(State.Open, null);

                    // after the connection is recovered we have to reconnect all the publishers and consumers
                    try
                    {
                        await ReconnectEntitiesAsync()
                            .ConfigureAwait(false);
                    }
                    catch (Exception e)
                    {
                        Trace.WriteLine(TraceLevel.Error, $"{ToString()} error trying to reconnect entities {e}");
                    }
                }
            }
            catch
            {
                // TODO set states to Closed? Error?
                // This will be skipped if reconnection succeeds, but if there
                // is an exception, it's important that this be called.
                _connectionCloseTaskCompletionSource.SetResult(true);
                throw;
            }
            finally
            {
                // TODO it is odd to have to add this code, figure out why
                try
                {
                    _semaphoreClose.Release();
                }
                catch (ObjectDisposedException)
                {
                }
            }
        };
    }

    private void ChangeEntitiesStatus(State state, Error? error)
    {
        ChangePublishersStatus(state, error);
        ChangeConsumersStatus(state, error);
        _management.ChangeStatus(state, error);
    }

    private void ChangePublishersStatus(State state, Error? error)
    {
        foreach (IPublisher publisher1 in Publishers.Values)
        {
            var publisher = (AmqpPublisher)publisher1;
            publisher.ChangeStatus(state, error);
        }
    }

    private void ChangeConsumersStatus(State state, Error? error)
    {
        foreach (IConsumer consumer1 in Consumers.Values)
        {
            var consumer = (AmqpConsumer)consumer1;
            consumer.ChangeStatus(state, error);
        }
    }

    private async Task ReconnectEntitiesAsync()
    {
        await ReconnectPublishersAsync()
            .ConfigureAwait(false);
        await ReconnectConsumersAsync()
            .ConfigureAwait(false);
    }

    private async Task ReconnectPublishersAsync()
    {
        // TODO this could be done in parallel
        foreach (IPublisher publisher1 in Publishers.Values)
        {
            var publisher = (AmqpPublisher)publisher1;
            await publisher.ReconnectAsync()
                .ConfigureAwait(false);
        }
    }

    private async Task ReconnectConsumersAsync()
    {
        // TODO this could be done in parallel
        foreach (IConsumer consumer1 in Consumers.Values)
        {
            var consumer = (AmqpConsumer)consumer1;
            await consumer.ReconnectAsync()
                .ConfigureAwait(false);
        }
    }
}

internal class Visitor(AmqpManagement management) : IVisitor
{
    private AmqpManagement Management { get; } = management;

    public async Task VisitQueuesAsync(IEnumerable<QueueSpec> queueSpec)
    {
        // TODO this could be done in parallel
        foreach (QueueSpec spec in queueSpec)
        {
            Trace.WriteLine(TraceLevel.Information, $"Recovering queue {spec.Name}");
            try
            {
                await Management.Queue(spec).DeclareAsync()
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
