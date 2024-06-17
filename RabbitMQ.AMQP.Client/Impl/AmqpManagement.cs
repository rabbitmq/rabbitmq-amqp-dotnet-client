using System.Collections.Concurrent;
using Amqp;
using Amqp.Framing;
using Amqp.Types;
using Trace = Amqp.Trace;
using TraceLevel = Amqp.TraceLevel;

namespace RabbitMQ.AMQP.Client.Impl;

/// <summary>
/// AmqpManagement implements the IManagement interface and is responsible for managing the AMQP resources.
/// RabbitMQ uses AMQP end  point: "/management" to manage the resources like queues, exchanges, and bindings.
/// The management endpoint works like an HTTP RPC endpoint where the client sends a request to the server
/// </summary>
public class AmqpManagement : IManagement
{
    private readonly ConcurrentDictionary<string, TaskCompletionSource<Message>> _requests = new();

    // private static readonly long IdSequence = 0;
    //
    private RecordingTopologyListener? _recordingTopologyListener;
    private const string ManagementNodeAddress = "/management";
    private const string LinkPairName = "management-link-pair";

    internal const int Code200 = 200;
    internal const int Code201 = 201;
    internal const int Code204 = 204; // TODO: handle 204
    internal const int Code409 = 409;
    internal const string Put = "PUT";
    internal const string Delete = "DELETE";

    private const string ReplyTo = "$me";


    public virtual Status Status { get; protected set; } = Status.Closed;


    public IQueueSpecification Queue()
    {
        return new AmqpQueueSpecification(this);
    }

    public IQueueSpecification Queue(string name)
    {
        return Queue().Name(name);
    }

    public IQueueSpecification Queue(QueueSpec spec)
    {
        return Queue().Name(spec.Name)
            .AutoDelete(spec.AutoDelete)
            .Exclusive(spec.Exclusive)
            .Arguments(spec.Arguments);
    }

    public IQueueDeletion QueueDeletion()
    {
        return new AmqpQueueDeletion(this);
    }

    public ITopologyListener TopologyListener()
    {
        return _recordingTopologyListener!;
    }

    private Session? _managementSession;
    private SenderLink? _senderLink;
    private ReceiverLink? _receiverLink;
    private AmqpConnection? _amqpConnection;


    internal void Init(AmqpManagementParameters parameters)
    {
        if (Status == Status.Open)
            return;

        _amqpConnection = parameters.Connection();
        if (_managementSession == null || _managementSession.IsClosed)
            _managementSession = new Session(_amqpConnection.NativeConnection());
        _recordingTopologyListener = parameters.TopologyListener();

        EnsureSenderLink();
        // by the Management implementation the sender link _must_ be open before the receiver link
        // this sleep is to ensure that the sender link is open before the receiver link
        // TODO: find a better way to ensure that the sender link is open before the receiver link
        Thread.Sleep(500);
        EnsureReceiverLink();
        _ = Task.Run(async () =>
        {
            try
            {
                while (_managementSession.IsClosed == false &&
                       _amqpConnection.NativeConnection()!.IsClosed == false)
                {
                    if (_receiverLink == null) continue;
                    var msg = await _receiverLink.ReceiveAsync();
                    if (msg == null)
                    {
                        Trace.WriteLine(TraceLevel.Warning, "Received null message");
                        continue;
                    }

                    _receiverLink.Accept(msg);
                    HandleResponseMessage(msg);
                    msg.Dispose();
                }
            }
            catch (Exception e)
            {
                Trace.WriteLine(TraceLevel.Error,
                    $"Receiver link error in management session {e}. Receiver link closed: {_receiverLink?.IsClosed}");
            }

            Trace.WriteLine(TraceLevel.Information, "AMQP Management session closed");
        });
        _managementSession.Closed += (sender, error) =>
        {
            Trace.WriteLine(TraceLevel.Warning, $"Management session closed " +
                                                $"sender: {sender} error: {error} " +
                                                $"Amqp Status:{Status} senderLink closed:  {_senderLink?.IsClosed}" +
                                                $"_receiverLink closed: {_receiverLink?.IsClosed} " +
                                                $"_managementSession is closed: {_managementSession.IsClosed}" +
                                                $"native connection is closed: {_amqpConnection.NativeConnection()!.IsClosed}");
            OnNewStatus(Status.Closed, Utils.ConvertError(error));
        };
        OnNewStatus(Status.Open, null);
    }


    private void EnsureReceiverLink()
    {
        if (_receiverLink == null || _receiverLink.IsClosed)
        {
            var receiveAttach = new Attach()
            {
                SndSettleMode = SenderSettleMode.Settled,
                RcvSettleMode = ReceiverSettleMode.First,
                Properties = new Fields
                {
                    { new Symbol("paired"), true }
                },
                LinkName = LinkPairName,
                Source = new Source()
                {
                    Address = ManagementNodeAddress,
                },
                Handle = 1,
                Target = new Target()
                {
                    Address = ManagementNodeAddress,
                },
            };
            _receiverLink = new ReceiverLink(
                _managementSession, LinkPairName, receiveAttach, null);

            _receiverLink.SetCredit(100);
        }
    }

    private void OnNewStatus(Status newStatus, Error? error)
    {
        var oldStatus = Status;
        Status = newStatus;
        ChangeStatus?.Invoke(this, oldStatus, Status, error);
    }

    private void EnsureSenderLink()
    {
        if (_senderLink == null || _senderLink.IsClosed)
        {
            var senderAttach = new Attach
            {
                SndSettleMode = SenderSettleMode.Settled,
                RcvSettleMode = ReceiverSettleMode.First,

                Properties = new Fields
                {
                    { new Symbol("paired"), true }
                },
                LinkName = LinkPairName,
                Source = new Source()
                {
                    Address = ManagementNodeAddress,
                    ExpiryPolicy = new Symbol("LINK_DETACH"),
                    Timeout = 0,
                    Dynamic = false,
                    Durable = 0
                },
                Handle = 0,
                Target = new Target()
                {
                    Address = ManagementNodeAddress,
                    ExpiryPolicy = new Symbol("SESSION_END"),
                    Timeout = 0,
                    Dynamic = false,
                },
            };
            _senderLink = new SenderLink(
                _managementSession, LinkPairName, senderAttach, null);
        }
    }

    protected void HandleResponseMessage(Message msg)
    {
        if (msg.Properties.CorrelationId != null &&
            _requests.TryRemove(msg.Properties.CorrelationId, out var mre))
        {
            if (mre.TrySetResult(msg))
            {
                Trace.WriteLine(TraceLevel.Information, $"Set result for  {msg.Properties.CorrelationId}");
            }
        }
        else
        {
            Trace.WriteLine(TraceLevel.Error, $"No request found for message {msg.Properties.CorrelationId}");
        }
    }

    internal async ValueTask<Message> Request(object? body, string path, string method,
        int[] expectedResponseCodes, TimeSpan? timeout = null)
    {
        var id = Guid.NewGuid().ToString();
        return await Request(id, body, path, method, expectedResponseCodes, timeout);
    }

    internal async ValueTask<Message> Request(string id, object? body, string path, string method,
        int[] expectedResponseCodes, TimeSpan? timeout = null)
    {
        var message = new Message(body);
        message.Properties = new Properties
        {
            MessageId = id,
            To = path,
            Subject = method,
            ReplyTo = ReplyTo
        };

        return await Request(message, expectedResponseCodes, timeout);
    }

    internal async ValueTask<Message> Request(Message message, int[] expectedResponseCodes, TimeSpan? timeout = null)
    {
        if (Status != Status.Open)
        {
            throw new ModelException("Management is not open");
        }

        TaskCompletionSource<Message> mre = new(TaskCreationOptions.RunContinuationsAsynchronously);
        _requests.TryAdd(message.Properties.MessageId, mre);
        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(5));
        await using (cts.Token.Register(
                         () =>
                         {
                             Trace.WriteLine(TraceLevel.Warning, $"Request timeout for {message.Properties.MessageId}");
                             _requests.TryRemove(message.Properties.MessageId, out _);
                         }))
        {
            await InternalSendAsync(message);
            var result = await mre.Task.WaitAsync(cts.Token);
            CheckResponse(message, expectedResponseCodes, result);
            return result;
        }
    }

    internal void CheckResponse(Message sentMessage, int[] expectedResponseCodes, Message receivedMessage)
    {
        if (!int.TryParse(receivedMessage.Properties.Subject, out var responseCode))
            throw new ModelException($"Response code is not a number {receivedMessage.Properties.Subject}");

        switch (responseCode)
        {
            case Code409:
                throw new PreconditionFailException($"Precondition Fail. Message: {receivedMessage.Body}");
        }

        if (sentMessage.Properties.MessageId != receivedMessage.Properties.CorrelationId)
            throw new ModelException(
                $"CorrelationId does not match, expected {sentMessage.Properties.MessageId} but got {receivedMessage.Properties.CorrelationId}");


        var any = expectedResponseCodes.Any(c => c == responseCode);
        if (!any)
        {
            throw new InvalidCodeException(
                $"Unexpected response code: {responseCode} instead of {expectedResponseCodes.Aggregate("", (s, i) => s + i + ", ")}"
            );
        }
    }

    protected virtual async Task InternalSendAsync(Message message)
    {
        await _senderLink!.SendAsync(message);
    }

    public async Task CloseAsync()
    {
        Status = Status.Closed;
        if (_managementSession is { IsClosed: false })
        {
            await _managementSession.CloseAsync();
        }
    }

    public event IClosable.ChangeStatusCallBack? ChangeStatus;
}

public class InvalidCodeException(string message) : Exception(message);