using System.Diagnostics;
using Amqp;
using Amqp.Framing;
using Amqp.Listener;
using Amqp.Types;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpPublisher : AbstractClosable, IPublisher
{
    private SenderLink _senderLink = null!;

    // private readonly ManualResetEvent _stopPublishing = new(true);
    private readonly AmqpConnection _connection;
    private readonly TimeSpan _timeout;
    private readonly string _address;

    public AmqpPublisher(AmqpConnection connection, string address, TimeSpan timeout)
    {
        _address = address;
        _connection = connection;
        _timeout = timeout;
        connection.Publishers.TryAdd(Id, this);
        Connect();
    }

    internal void Connect()
    {
        try
        {
            var complete = new ManualResetEvent(false);
            _senderLink = new SenderLink(_connection.NativePubSubSessions.GetOrCreateSession(), Id,
                Utils.CreateSenderAttach(_address, DeliveryMode.AtLeastOnce, Id),
                (link, attach) =>
                {
                    complete.Set();
                });
            complete.WaitOne(TimeSpan.FromSeconds(5));
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
        // _stopPublishing.Reset();
    }

    public void ResumePublishing()
    {
        // _stopPublishing.Set();
    }


    // protected override async Task<int> ExecuteAsync(SenderLink link)
    // {
    //     int batch = this.random.Next(1, this.role.Args.Batch);
    //     Message[] messages = CreateMessages(this.id, this.total, batch);
    //     await Task.WhenAll(messages.Select(m => link.SendAsync(m)));
    //     this.total += batch;
    //     return batch;
    // }

    public Task Publish(IMessage message, OutcomeDescriptorCallback outcomeCallback)
    {
        ThrowIfClosed();
        try
        {
            // _stopPublishing.WaitOne(_timeout);
            var nMessage = ((AmqpMessage)message).NativeMessage;
            _senderLink.Send(nMessage,
                (sender, outMessage, outcome, state) =>
                {
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
            await _senderLink.CloseAsync();
        }
        catch (Exception e)
        {
            Trace.WriteLine(TraceLevel.Warning, "Failed to close sender link. The publisher will be closed anyway", e);
        }

        OnNewStatus(State.Closed, null);
    }
}