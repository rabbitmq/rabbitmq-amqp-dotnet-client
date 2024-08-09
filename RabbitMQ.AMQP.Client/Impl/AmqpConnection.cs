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
    internal ConcurrentDictionary<string, IPublisher> Publishers { get; } = new();
    internal ConcurrentDictionary<string, IConsumer> Consumers { get; } = new();

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

    public override async Task OpenAsync()
    {
        await OpenConnectionAsync()
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
            await CloseAllPublishers().ConfigureAwait(false);
            await CloseAllConsumers().ConfigureAwait(false);

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

        await ConnectionCloseTaskCompletionSource.Task.WaitAsync(TimeSpan.FromSeconds(10))
            .ConfigureAwait(false);

        OnNewStatus(State.Closed, null);
    }

    public override string ToString()
    {
        string info = $"AmqpConnection{{ConnectionSettings='{_connectionSettings}', Status='{State.ToString()}'}}";
        return info;
    }

    internal Connection? NativeConnection()
    {
        return _nativeConnection;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            // TODO probably more should/could happen in this method
            _semaphoreOpen.Dispose();
            _semaphoreClose.Dispose();
        }

        base.Dispose(disposing);
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

    private AmqpConnection(IConnectionSettings connectionSettings)
    {
        _connectionSettings = connectionSettings;
        _nativePubSubSessions = new AmqpSessionManagement(this, 1);
        _management =
            new AmqpManagement(new AmqpManagementParameters(this).TopologyListener(_recordingTopologyListener));
    }

    // TODO cancellation token
    private async Task OpenConnectionAsync()
    {
        await _semaphoreOpen.WaitAsync()
            .ConfigureAwait(false);
        try
        {
            if (_nativeConnection is { IsClosed: false })
            {
                return;
            }

            var open = new Open
            {
                HostName = $"vhost:{_connectionSettings.VirtualHost}",
                Properties = new Fields()
                {
                    [new Symbol("connection_name")] = _connectionSettings.ConnectionName,
                }
            };

            if (_connectionSettings.MaxFrameSize > uint.MinValue)
            {
                // Note: when set here, there is no need to set cf.AMQP.MaxFrameSize
                open.MaxFrameSize = _connectionSettings.MaxFrameSize;
            }

            void onOpened(Amqp.IConnection connection, Open open1)
            {
                Trace.WriteLine(TraceLevel.Verbose, $"Connection opened. Info: {ToString()}");
                OnNewStatus(State.Open, null);
            }

            var cf = new ConnectionFactory();

            if (_connectionSettings.UseSsl && _connectionSettings.TlsSettings is not null)
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

            try
            {
                _nativeConnection = await cf.CreateAsync((_connectionSettings as ConnectionSettings)?.Address, open: open, onOpened: onOpened)
                    .ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                throw new ConnectionException(
                    $"Connection failed. Info: {ToString()}", ex);
            }

            if (_nativeConnection.IsClosed)
            {
                throw new ConnectionException(
                    $"Connection failed. Info: {ToString()}, error: {_nativeConnection.Error}");
            }

            await _management.OpenAsync()
                .ConfigureAwait(false);

            _nativeConnection.Closed += MaybeRecoverConnection();
        }
        catch (AmqpException e)
        {
            Trace.WriteLine(TraceLevel.Error, $"Error trying to connect. Info: {ToString()}, error: {e}");
            throw new ConnectionException($"Error trying to connect. Info: {ToString()}, error: {e}");
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
    private ClosedCallback MaybeRecoverConnection()
    {
        return async (sender, error) =>
        {
            await _semaphoreClose.WaitAsync()
                .ConfigureAwait(false);
            try
            {
                // close all the sessions, if the connection is closed the sessions are not valid anymore
                _nativePubSubSessions.ClearSessions();

                if (error != null)
                {
                    //  we assume here that the connection is closed unexpectedly, since the error is not null
                    Trace.WriteLine(TraceLevel.Warning, $"connection is closed unexpectedly. " +
                                                        $"Info: {ToString()}");

                    // we have to check if the recovery is active.
                    // The user may want to disable the recovery mechanism
                    // the user can use the lifecycle callback to handle the error
                    if (!_connectionSettings.Recovery.IsActivate())
                    {
                        OnNewStatus(State.Closed, Utils.ConvertError(error));
                        ChangeEntitiesStatus(State.Closed, Utils.ConvertError(error));
                        return;
                    }

                    // change the status for the connection and all the entities
                    // to reconnecting and all the events are fired
                    OnNewStatus(State.Reconnecting, Utils.ConvertError(error));
                    ChangeEntitiesStatus(State.Reconnecting, Utils.ConvertError(error));

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
                               _connectionSettings.Recovery.GetBackOffDelayPolicy().IsActive() &&

                               // even we set the status to reconnecting up, we need to check if the connection is still in the
                               // reconnecting status. The user may close the connection in the meanwhile
                               State == State.Reconnecting)
                        {
                            try
                            {
                                int nextDelayMs = _connectionSettings.Recovery.GetBackOffDelayPolicy().Delay();

                                Trace.WriteLine(TraceLevel.Information,
                                    $"Trying Recovering connection in {nextDelayMs} milliseconds, " +
                                    $"attempt: {_connectionSettings.Recovery.GetBackOffDelayPolicy().CurrentAttempt}. " +
                                    $"Info: {ToString()})");

                                await Task.Delay(TimeSpan.FromMilliseconds(nextDelayMs))
                                    .ConfigureAwait(false);

                                await OpenConnectionAsync()
                                    .ConfigureAwait(false);

                                connected = true;
                            }
                            catch (Exception e)
                            {
                                Trace.WriteLine(TraceLevel.Warning,
                                    $"Error trying to recover connection {e}. Info: {this}");
                            }
                        }

                        _connectionSettings.Recovery.GetBackOffDelayPolicy().Reset();
                        string connectionDescription = connected ? "recovered" : "not recovered";
                        Trace.WriteLine(TraceLevel.Information,
                            $"Connection {connectionDescription}. Info: {ToString()}");

                        if (!connected)
                        {
                            Trace.WriteLine(TraceLevel.Verbose, $"connection is closed. Info: {ToString()}");
                            OnNewStatus(State.Closed,
                                new Error(ConnectionNotRecoveredCode,
                                    $"{ConnectionNotRecoveredMessage}, recover status: {_connectionSettings.Recovery}"));

                            ChangeEntitiesStatus(State.Closed, new Error(ConnectionNotRecoveredCode,
                                $"{ConnectionNotRecoveredMessage}, recover status: {_connectionSettings.Recovery}"));

                            return;
                        }

                        if (_connectionSettings.Recovery.IsTopologyActive())
                        {
                            Trace.WriteLine(TraceLevel.Information, $"Recovering topology. Info: {ToString()}");
                            var visitor = new Visitor(_management);
                            await _recordingTopologyListener.Accept(visitor)
                                .ConfigureAwait(false);
                        }

                        OnNewStatus(State.Open, null);
                        // after the connection is recovered we have to reconnect all the publishers and consumers

                        try
                        {
                            await ReconnectEntitiesAsync().ConfigureAwait(false);
                        }
                        catch (Exception e)
                        {
                            Trace.WriteLine(TraceLevel.Error, $"Error trying to reconnect entities {e}. Info: {this}");
                        }
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

            ConnectionCloseTaskCompletionSource.SetResult(true);
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
            await consumer.ReconnectAsync().ConfigureAwait(false);
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
