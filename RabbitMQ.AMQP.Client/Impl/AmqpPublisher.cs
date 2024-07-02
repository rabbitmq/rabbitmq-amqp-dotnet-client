using System.Diagnostics;
using Amqp;
using Amqp.Framing;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpPublisher : AbstractClosable, IPublisher
{
    private SenderLink _senderLink = null!;

    private readonly ManualResetEvent _pausePublishing = new(true);
    private readonly AmqpConnection _connection;
    private readonly TimeSpan _timeout;
    private readonly string _address;
    private readonly int _maxInFlight;

    public AmqpPublisher(AmqpConnection connection, string address, TimeSpan timeout, int maxInFlight)
    {
        _address = address;
        _connection = connection;
        _timeout = timeout;
        _maxInFlight = maxInFlight;
        Connect();
        connection.Publishers.TryAdd(Id, this);
    }

    private void Connect()
    {
        try
        {
            var attachCompleted = new ManualResetEvent(false);
            _senderLink = new SenderLink(_connection.NativePubSubSessions.GetOrCreateSession(), Id,
                Utils.CreateSenderAttach(_address, DeliveryMode.AtLeastOnce, Id),
                (link, attach) => { attachCompleted.Set(); });
            attachCompleted.WaitOne(TimeSpan.FromSeconds(5));
            if (_senderLink.LinkState != LinkState.Attached)
            {
                throw new PublisherException("Failed to create sender link. Link state is not attached, error: " +
                    _senderLink.Error?.ToString() ?? "Unknown error");
            }

            OnNewStatus(State.Open, null);
        }
        catch (Exception e)
        {
            throw new PublisherException($"Failed to create sender link, {e}");
        }
    }

    public string Id { get; } = Guid.NewGuid().ToString();


    public void PausePublishing()
    {
        _pausePublishing.Reset();
    }

    private void MaybeResumePublishing()
    {
        if (State is State.Closing or State.Closed or State.Reconnecting)
        {
            return;
        }
        // Can be resumed only if the publisher is open and the in-flight messages are less than the max allowed
        // In case the publisher is closed, closing or reconnecting, the publishing will be paused
        _pausePublishing.Set();
    }

    private void MaybeBackPressure()
    {
        if (_currentInFlight >= _maxInFlight)
        {
            PausePublishing();
        }
        else
        {
            MaybeResumePublishing();
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

    private int _currentInFlight = 0;
    public Task Publish(IMessage message, OutcomeDescriptorCallback outcomeCallback)
    {
        ThrowIfClosed();
        try
        {
            _pausePublishing.WaitOne(_timeout);
            Interlocked.Increment(ref _currentInFlight);

            var nMessage = ((AmqpMessage)message).NativeMessage;
            _senderLink.Send(nMessage,
                (sender, outMessage, outcome, state) =>
                {
                    Interlocked.Decrement(ref _currentInFlight);
                    MaybeBackPressure();

                    if (outMessage == nMessage &&
                        outMessage.GetEstimatedMessageSize() == nMessage.GetEstimatedMessageSize())
                    {
                        if (outcome is Rejected rejected)
                        {
                            outcomeCallback(message, new OutcomeDescriptor(rejected.Descriptor.Code,
                                rejected.Descriptor.ToString(),
                                OutcomeState.Failed, Utils.ConvertError(rejected?.Error)));
                        }
                        else
                        {
                            outcomeCallback(message, new OutcomeDescriptor(outcome.Descriptor.Code,
                                outcome.Descriptor.ToString(),
                                OutcomeState.Accepted, null));
                        }
                    }
                    else
                    {
                        Trace.WriteLine(TraceLevel.Error, "Message not sent. Killing the process.");
                        Process.GetCurrentProcess().Kill();
                    }

                    // is it correct to dispose the message here?
                    // maybe we should expose a method to dispose the message
                    nMessage.Dispose();
                }, this);

        }
        catch (Exception e)
        {
            throw new PublisherException($"Failed to publish message, {e}");
        }

        return Task.CompletedTask;
    }


    public override async Task CloseAsync()
    {
        if (State == State.Closed)
        {
            return;
        }

        OnNewStatus(State.Closing, null);

        _connection.Publishers.TryRemove(Id, out _);

        try
        {
            await _senderLink.CloseAsync()
                .ConfigureAwait(false);
        }
        catch (Exception e)
        {
            Trace.WriteLine(TraceLevel.Warning, "Failed to close sender link. The publisher will be closed anyway", e);
        }

        OnNewStatus(State.Closed, null);
    }

    public void ResumePublishing()
    {
        MaybeResumePublishing();
    }
}
