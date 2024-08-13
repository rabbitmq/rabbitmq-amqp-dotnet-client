// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using Amqp;
using Amqp.Framing;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpPublisher : AbstractReconnectLifeCycle, IPublisher
{
    private readonly AmqpConnection _connection;
    private readonly TimeSpan _timeout;
    private readonly string _address;
    private readonly int _maxInFlight;
    private readonly Guid _id = Guid.NewGuid();

    private SenderLink? _senderLink = null;

    public AmqpPublisher(AmqpConnection connection, string address, TimeSpan timeout, int maxInFlight)
    {
        _connection = connection;
        _address = address;
        _timeout = timeout;
        _maxInFlight = maxInFlight;

        if (false == _connection.Publishers.TryAdd(_id, this))
        {
            // TODO error?
        }
    }

    public override async Task OpenAsync()
    {
        try
        {
            TaskCompletionSource<SenderLink> attachCompletedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

            Attach attach = Utils.CreateAttach(_address, DeliveryMode.AtLeastOnce, _id);

            void onAttached(ILink argLink, Attach argAttach)
            {
                if (argLink is SenderLink link)
                {
                    attachCompletedTcs.SetResult(link);
                }
                else
                {
                    // TODO create "internal bug" exception type?
                    var ex = new InvalidOperationException(
                        "invalid link in onAttached, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
                    attachCompletedTcs.SetException(ex);
                }
            }

            SenderLink? tmpSenderLink = null;
            Task senderLinkTask = Task.Run(() =>
            {
                tmpSenderLink = new SenderLink(_connection._nativePubSubSessions.GetOrCreateSession(), _id.ToString(), attach, onAttached);
            });

            // TODO configurable timeout
            TimeSpan waitSpan = TimeSpan.FromSeconds(5);

            _senderLink = await attachCompletedTcs.Task.WaitAsync(waitSpan)
                .ConfigureAwait(false);

            await senderLinkTask.WaitAsync(waitSpan)
                .ConfigureAwait(false);

            System.Diagnostics.Debug.Assert(tmpSenderLink != null);
            System.Diagnostics.Debug.Assert(Object.ReferenceEquals(_senderLink, tmpSenderLink));

            if (_senderLink is null)
            {
                throw new PublisherException($"{ToString()} Failed to create sender link (null was returned)");
            }
            else if (_senderLink.LinkState != LinkState.Attached)
            {
                throw new PublisherException($"{ToString()} Failed to create sender link. Link state is not attached, error: " +
                    _senderLink.Error?.ToString() ?? "Unknown error");
            }
            else
            {
                await base.OpenAsync()
                    .ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            throw new PublisherException($"{ToString()} Failed to create sender link, {e}");
        }
    }

    // TODO: Consider implementing this method with the send method
    // a way to send a batch of messages

    // protected override async Task<int> ExecuteAsync(SenderLink link)
    // {
    //     int batch = this.random.Next(1, this.role.Args.Batch);
    //     Message[] messages = CreateMessages(this.id, this.total, batch);
    //     await Task.WhenAll(messages.Select(m => link.SendAsync(m)));
    //     this.total += batch;
    //     return batch;
    // }

    public async Task<PublishResult> PublishAsync(IMessage message, CancellationToken cancellationToken = default)
    {
        ThrowIfClosed();

        if (_senderLink is null)
        {
            // TODO create "internal bug" exception type?
            throw new InvalidOperationException("_senderLink is null, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
        }

        try
        {
            TaskCompletionSource<PublishOutcome> messagePublishedTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
            Message nativeMessage = ((AmqpMessage)message).NativeMessage;

            void OutcomeCallback(ILink sender, Message message, Outcome outcome, object state)
            {
                System.Diagnostics.Debug.Assert(Object.ReferenceEquals(this, state));
                System.Diagnostics.Debug.Assert(Object.ReferenceEquals(_senderLink, sender));
                // Note: sometimes `message` is null 🤔
                // System.Diagnostics.Debug.Assert(Object.ReferenceEquals(nativeMessage, message));

                OutcomeState publishState = OutcomeState.Accepted;
                // TODO what about other outcomes, like Released?
                if (outcome is Rejected rejectedOutcome)
                {
                    publishState = OutcomeState.Failed;
                }

                var publishOutcome = new PublishOutcome(publishState, null);
                messagePublishedTcs.SetResult(publishOutcome);
            }

            /*
             * Note: do NOT use SendAsync here as it prevents the Closed event from
             * firing on the native connection. Bizarre, I know!
             */
            _senderLink.Send(nativeMessage, OutcomeCallback, this);

            // TODO operation timeout
            PublishOutcome publishOutcome = await messagePublishedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5), cancellationToken)
                .ConfigureAwait(false);

            return new PublishResult(message, publishOutcome);
        }
        catch (AmqpException ex)
        {
            var publishOutcome = new PublishOutcome(OutcomeState.Failed, Utils.ConvertError(ex.Error));
            return new PublishResult(message, publishOutcome);
        }
        catch (Exception e)
        {
            throw new PublisherException($"{ToString()} Failed to publish message, {e}");
        }
    }

    public override async Task CloseAsync()
    {
        // TODO make this method similar to consumer CloseAsync
        if (State == State.Closed)
        {
            return;
        }

        OnNewStatus(State.Closing, null);

        try
        {
            if (_senderLink != null)
            {
                await _senderLink.DetachAsync()
                    .ConfigureAwait(false);
                await _senderLink.CloseAsync()
                    .ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            Trace.WriteLine(TraceLevel.Warning, "Failed to close sender link. The publisher will be closed anyway", e);
        }

        _senderLink = null;
        OnNewStatus(State.Closed, null);
        _connection.Publishers.TryRemove(_id, out _);
    }

    public override string ToString()
    {
        return $"Publisher{{Address='{_address}', id={_id} Connection='{_connection}', State='{State}'}}";
    }
}
