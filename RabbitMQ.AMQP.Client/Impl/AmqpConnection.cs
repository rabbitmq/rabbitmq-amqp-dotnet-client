// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amqp;
using Amqp.Framing;
using Amqp.Sasl;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl
{
    /// <summary>
    /// <see cref="AmqpConnection"/> is the concrete implementation of <see cref="IConnection"/>.
    /// It is a wrapper around the Microsoft AMQP.Net Lite <see cref="Amqp.Connection"/> class.
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

        private readonly ConnectionSettings _connectionSettings;
        private readonly IMetricsReporter? _metricsReporter;

        private readonly Dictionary<string, object> _connectionProperties = new();
        internal readonly FeatureFlags _featureFlags = new();

        /// <summary>
        /// _publishersDict contains all the publishers created by the connection.
        /// Each connection can have multiple publishers.
        /// They key is the publisher Id (a Guid)
        /// See <see cref="AmqpPublisher"/>
        /// </summary>
        private readonly ConcurrentDictionary<Guid, IPublisher> _publishersDict = new();

        /// <summary>
        /// _consumersDict contains all the publishers created by the connection.
        /// Each connection can have multiple consumers.
        /// They key is the consumers Id (a Guid)
        /// See <see cref="AmqpConsumer"/>
        /// </summary>
        private readonly ConcurrentDictionary<Guid, IConsumer> _consumersDict = new();

        private readonly TaskCompletionSource<bool> _connectionClosedTcs =
            Utils.CreateTaskCompletionSource<bool>();

        // TODO this is coupled with publishers and consumers
        internal readonly AmqpSessionManagement _nativePubSubSessions;

        /// <summary>
        /// <para>
        ///   Creates a new instance of <see cref="AmqpConnection"/>
        /// </para>
        /// <para>
        ///   Through this <see cref="IConnection"/> instance it is possible to create:
        /// <list type="bullet">
        ///   <item>
        ///     <see cref="IManagement"/>: See <see cref="AmqpManagement"/>.
        ///   </item>
        ///   <item>
        ///     <see cref="IPublisher"/> and <see cref="IConsumer"/>. See <see cref="AmqpPublisherBuilder"/> and <see cref="AmqpConsumerBuilder"/>.
        ///   </item>
        /// </list>
        /// </para>
        /// </summary>
        /// <param name="connectionSettings"></param>
        /// <param name="metricsReporter"></param>
        /// <returns></returns>
        // TODO to play nicely with IoC containers, we should not have static Create methods
        // TODO rename to CreateAndOpenAsync
        public static async Task<IConnection> CreateAsync(ConnectionSettings connectionSettings,
            IMetricsReporter? metricsReporter = default)
        {
            AmqpConnection connection = new(connectionSettings, metricsReporter);
            await connection.OpenAsync()
                .ConfigureAwait(false);

            return connection;
        }

        /// <summary>
        /// The <see cref="IManagement"/> instance for this connection.
        /// </summary>
        /// <returns><see cref="IManagement"/> instance for this connection.</returns>
        public IManagement Management()
        {
            return _management;
        }

        /// <summary>
        /// Create an <see cref="IPublisherBuilder"/> instance for this connection.
        /// </summary>
        /// <returns><see cref="IPublisherBuilder"/> instance for this connection.</returns>
        public IPublisherBuilder PublisherBuilder()
        {
            ThrowIfClosed();
            var publisherBuilder = new AmqpPublisherBuilder(this, _metricsReporter);
            return publisherBuilder;
        }

        /// <summary>
        /// Create an <see cref="IConsumerBuilder"/> instance for this connection.
        /// </summary>
        /// <returns><see cref="IConsumerBuilder"/> instance for this connection.</returns>
        public IConsumerBuilder ConsumerBuilder()
        {
            return new AmqpConsumerBuilder(this, _metricsReporter);
        }

        /// <summary>
        /// Create an <see cref="IResponderBuilder"/> instance for this connection.
        /// </summary>
        /// <returns><see cref="IResponderBuilder"/> instance for this connection.</returns>
        public IResponderBuilder ResponderBuilder()
        {
            return new AmqpResponderBuilder(this);
        }

        /// <summary>
        /// Create an <see cref="IRequesterBuilder"/> instance for this connection.
        /// </summary>
        /// <returns><see cref="IRequesterBuilder"/> instance for this connection.</returns>
        public IRequesterBuilder RequesterBuilder()
        {
            return new AmqpRequesterBuilder(this);
        }

        /// <summary>
        /// Get the properties for this connection.
        /// </summary>
        /// <returns><see cref="IReadOnlyDictionary{TKey, TValue}"/> of connection properties.</returns>
        public IReadOnlyDictionary<string, object> Properties => _connectionProperties;

        /// <summary>
        /// Get the <see cref="IPublisher"/> instances associated with this connection.
        /// </summary>
        /// <returns><see cref="IEnumerable{T}"/> of <see cref="IPublisher"/> instances.</returns>
        public IEnumerable<IPublisher> Publishers
            => _publishersDict.Values.ToArray();

        /// <summary>
        /// Get the <see cref="IConsumer"/> instances associated with this connection.
        /// </summary>
        /// <returns><see cref="IEnumerable{T}"/> of <see cref="IConsumer"/> instances.</returns>
        public IEnumerable<IConsumer> Consumers
            => _consumersDict.Values.ToArray();

        /// <summary>
        /// Get or set the Connection ID. Used by <see cref="AmqpEnvironment"/>
        /// </summary>
        public long Id { get; set; }

        /// <summary>
        /// Refresh the OAuth token and update the connection settings.
        /// </summary>
        /// <param name="token">OAuth token</param>
        public async Task RefreshTokenAsync(string token)
        {
            // here we use the primitive RequestAsync method because we don't want to
            // expose this method in the IManagement interface
            // we need to update the connection settings with the new token
            int[] expectedResponseCodes = { AmqpManagement.Code204 };
            _ = await _management.RequestAsync(Encoding.ASCII.GetBytes(token),
                    AmqpManagement.AuthTokens, AmqpManagement.Put, expectedResponseCodes)
                .ConfigureAwait(false);
            _connectionSettings.UpdateOAuthPassword(token);
        }

        // TODO cancellation token
        public override async Task OpenAsync()
        {
            // TODO cancellation token
            await OpenConnectionAsync(CancellationToken.None)
                .ConfigureAwait(false);
            await base.OpenAsync()
                .ConfigureAwait(false);
            _featureFlags.Validate();
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

            // TODO configurable timeout?
            await _connectionClosedTcs.Task.WaitAsync(TimeSpan.FromSeconds(10))
                .ConfigureAwait(false);

            OnNewStatus(State.Closed, null);
        }

        public override string ToString()
        {
            string info = $"AmqpConnection{{ConnectionSettings='{_connectionSettings}', Status='{State.ToString()}'}}";
            return info;
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

        internal Connection? NativeConnection => _nativeConnection;

        // TODO this couples AmqpConnection with AmqpPublisher, yuck
        internal void AddPublisher(Guid id, IPublisher consumer)
        {
            if (false == _publishersDict.TryAdd(id, consumer))
            {
                // TODO create "internal bug" exception type?
                throw new InvalidOperationException(
                    "could not add publisher, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
            }
        }

        internal void RemovePublisher(Guid id)
        {
            if (false == _publishersDict.TryRemove(id, out _))
            {
                // TODO create "internal bug" exception type?
                throw new InvalidOperationException(
                    "could not remove publisher, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
            }
        }

        // TODO this couples AmqpConnection with AmqpConsumer, yuck
        internal void AddConsumer(Guid id, IConsumer consumer)
        {
            if (false == _consumersDict.TryAdd(id, consumer))
            {
                // TODO create "internal bug" exception type?
                throw new InvalidOperationException(
                    "could not add consumer, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
            }
        }

        internal void RemoveConsumer(Guid id)
        {
            if (false == _consumersDict.TryRemove(id, out _))
            {
                // TODO create "internal bug" exception type?
                throw new InvalidOperationException(
                    "could not remove consumer, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
            }
        }

        /// <summary>
        /// Closes all the publishers. It is called when the connection is closed.
        /// </summary>
        // TODO cancellation token, parallel?
        private async Task CloseAllPublishersAsync()
        {
            foreach (IPublisher publisher in Publishers)
            {
                await publisher.CloseAsync()
                    .ConfigureAwait(false);
            }
        }

        // TODO cancellation token, parallel?
        private async Task CloseAllConsumersAsync()
        {
            foreach (IConsumer consumer in Consumers)
            {
                await consumer.CloseAsync()
                    .ConfigureAwait(false);
            }
        }

        private AmqpConnection(ConnectionSettings connectionSettings, IMetricsReporter? metricsReporter)
        {
            _connectionSettings = connectionSettings;
            _metricsReporter = metricsReporter;
            _nativePubSubSessions = new AmqpSessionManagement(this, 1);
            _management =
                new AmqpManagement(new AmqpManagementParameters(this).TopologyListener(_recordingTopologyListener));
        }

        private async Task OpenConnectionAsync(CancellationToken cancellationToken)
        {
            await _semaphoreOpen.WaitAsync(cancellationToken)
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
                    Properties = new Fields() { [new Symbol("connection_name")] = _connectionSettings.ContainerId, }
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

                if (_connectionSettings.SaslMechanism == SaslMechanism.Anonymous)
                {
                    cf.SASL.Profile = SaslProfile.Anonymous;
                }
                else if (_connectionSettings.SaslMechanism == SaslMechanism.External)
                {
                    cf.SASL.Profile = SaslProfile.External;
                }

                void OnOpened(Amqp.IConnection connection, Open openOnOpened)
                {
                    HandleProperties(openOnOpened.Properties);
                    Trace.WriteLine(TraceLevel.Verbose, $"{ToString()} is open");
                    OnNewStatus(State.Open, null);
                }

                try
                {
                    Address address = _connectionSettings.Address;
                    _nativeConnection = await cf.CreateAsync(address: address, open: open, onOpened: OnOpened)
                        .ConfigureAwait(false);
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
                        _connectionClosedTcs.SetResult(true);
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
                        if (false == _connectionSettings.Recovery.IsActivated())
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
                    // TODO set states to Closed? Error? Log exception? Set exception on TCS?
                    // This will be skipped if reconnection succeeds, but if there
                    // is an exception, it's important that this be called.
                    _connectionClosedTcs.SetResult(true);
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
            foreach (IPublisher publisher1 in Publishers)
            {
                var publisher = (AmqpPublisher)publisher1;
                publisher.ChangeStatus(state, error);
            }
        }

        private void ChangeConsumersStatus(State state, Error? error)
        {
            foreach (IConsumer consumer1 in Consumers)
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
            foreach (IPublisher publisher1 in Publishers)
            {
                var publisher = (AmqpPublisher)publisher1;
                await publisher.ReconnectAsync()
                    .ConfigureAwait(false);
            }
        }

        private async Task ReconnectConsumersAsync()
        {
            // TODO this could be done in parallel
            foreach (IConsumer consumer1 in Consumers)
            {
                var consumer = (AmqpConsumer)consumer1;
                await consumer.ReconnectAsync()
                    .ConfigureAwait(false);
            }
        }

        private void HandleProperties(Fields properties)
        {
            foreach (KeyValuePair<object, object> kvp in properties)
            {
                string key = (Symbol)kvp.Key;
                string value = string.Empty;
                if (kvp.Value is not null)
                {
                    value = (string)kvp.Value;
                }

                _connectionProperties[key] = value;
            }

            string brokerVersion = (string)_connectionProperties["version"];
            _featureFlags.IsBrokerCompatible = Utils.Is4_0_OrMore(brokerVersion);

            // check if the broker supports filter expressions
            // this is a feature that was introduced in RabbitMQ 4.2.0
            _featureFlags.IsSqlFeatureEnabled = Utils.Is4_2_OrMore(brokerVersion);

            _featureFlags.IsDirectReplyToSupported = Utils.Is4_2_OrMore(brokerVersion);

            _featureFlags.IsFilterFeatureEnabled = Utils.SupportsFilterExpressions(brokerVersion);
        }
    }
}
