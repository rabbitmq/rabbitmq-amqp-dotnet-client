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
public class AmqpManagement : AbstractClosable, IManagement // TODO: Implement ToString()
{
    // The requests are stored in a dictionary with the correlationId as the key
    // The correlationId is used to match the request with the response
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


    public IQueueSpecification Queue()
    {
        ThrowIfClosed();
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

    public IExchangeSpecification Exchange()
    {
        ThrowIfClosed();
        return new AmqpExchangeSpecification(this);
    }

    public IExchangeSpecification Exchange(string name)
    {
        return Exchange().Name(name);
    }

    public IExchangeDeletion ExchangeDeletion()
    {
        return new AmqpExchangeDeletion(this);
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
        if (State == State.Open)
        {
            return;
        }

        _amqpConnection = parameters.Connection();

        if (_managementSession == null || _managementSession.IsClosed)
        {
            _managementSession = new Session(_amqpConnection.NativeConnection());
        }

        _recordingTopologyListener = parameters.TopologyListener();

        EnsureSenderLink();

        // by the Management implementation the sender link _must_ be open before the receiver link
        // this sleep is to ensure that the sender link is open before the receiver link
        // TODO: find a better way to ensure that the sender link is open before the receiver link
        Thread.Sleep(500);

        EnsureReceiverLink();

        _ = Task.Run(async () => { await ProcessResponses().ConfigureAwait(false); });

        _managementSession.Closed += (sender, error) =>
        {
            Trace.WriteLine(TraceLevel.Warning, $"Management session closed " +
                                                $"sender: {sender} error: {error} " +
                                                $"Amqp Status:{State} senderLink closed:  {_senderLink?.IsClosed}" +
                                                $"_receiverLink closed: {_receiverLink?.IsClosed} " +
                                                $"_managementSession is closed: {_managementSession.IsClosed}" +
                                                $"native connection is closed: {_amqpConnection.NativeConnection()!.IsClosed}");
            OnNewStatus(State.Closed, Utils.ConvertError(error));
        };
        OnNewStatus(State.Open, null);
    }

    private async Task ProcessResponses()
    {
        try
        {
            while (_managementSession?.IsClosed == false &&
                   _amqpConnection?.NativeConnection()!.IsClosed == false)
            {
                if (_receiverLink == null)
                {
                    continue;
                }

                using (Message msg = await _receiverLink.ReceiveAsync().ConfigureAwait(false))
                {
                    if (msg == null)
                    {
                        Trace.WriteLine(TraceLevel.Warning, "Received null message");
                        continue;
                    }

                    _receiverLink.Accept(msg);
                    HandleResponseMessage(msg);
                }
            }
        }
        catch (Exception e)
        {
            Trace.WriteLine(TraceLevel.Error,
                $"Receiver link error in management session {e}. Receiver link closed: {_receiverLink?.IsClosed}");
        }

        Trace.WriteLine(TraceLevel.Information, "AMQP Management session closed");
    }


    private void EnsureReceiverLink()
    {
        if (_receiverLink == null || _receiverLink.IsClosed)
        {
            var receiveAttach = new Attach()
            {
                SndSettleMode = SenderSettleMode.Settled,
                RcvSettleMode = ReceiverSettleMode.First,
                Properties = new Fields { { new Symbol("paired"), true } },
                LinkName = LinkPairName,
                Source = new Source() { Address = ManagementNodeAddress, ExpiryPolicy = new Symbol("LINK_DETACH"), },
                Handle = 1,
                Target = new Target() { Address = ManagementNodeAddress, ExpiryPolicy = new Symbol("SESSION_END"), },
            };
            _receiverLink = new ReceiverLink(
                _managementSession, LinkPairName, receiveAttach, null);

            _receiverLink.SetCredit(1);
        }
    }


    private void EnsureSenderLink()
    {
        if (_senderLink == null || _senderLink.IsClosed)
        {
            var senderAttach = new Attach
            {
                SndSettleMode = SenderSettleMode.Settled,
                RcvSettleMode = ReceiverSettleMode.First,
                Properties = new Fields { { new Symbol("paired"), true } },
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
                Trace.WriteLine(TraceLevel.Verbose, $"Set result for:  {msg.Properties.CorrelationId}");
            }
        }
        else
        {
            Trace.WriteLine(TraceLevel.Error, $"No request found for message: {msg.Properties.CorrelationId}");
        }
    }

    internal ValueTask<Message> Request(object? body, string path, string method,
        int[] expectedResponseCodes, TimeSpan? timeout = null)
    {
        string id = Guid.NewGuid().ToString();
        return Request(id, body, path, method, expectedResponseCodes, timeout);
    }

    internal ValueTask<Message> Request(string id, object? body, string path, string method,
        int[] expectedResponseCodes, TimeSpan? timeout = null)
    {
        var message = new Message(body)
        {
            Properties = new Properties { MessageId = id, To = path, Subject = method, ReplyTo = ReplyTo }
        };

        return Request(message, expectedResponseCodes, timeout);
    }

    /// <summary>
    /// Core function to send a request and wait for the response
    /// The request is an AMQP message with the following properties:
    /// - Properties.MessageId: Mandatory to identify the request
    /// - Properties.To: The path of the request, for example "/queues/my-queue"
    /// - Properties.Subject: The method of the request, for example "PUT"
    /// - Properties.ReplyTo: The address where the response will be sent. Default is: "$me"
    /// - Body: The body of the request. For example the QueueSpec to create a queue
    /// </summary>
    /// <param name="message">Request Message. Contains all the info to create/delete a resource</param>
    /// <param name="expectedResponseCodes"> The response codes expected for a specific call. See Code* Constants </param>
    /// <param name="timeout"> Default timeout for a request </param>
    /// <returns> A message with the Info response. For example in case of Queue creation is DefaultQueueInfo </returns>
    /// <exception cref="ModelException"> Application errors, see <see cref="ModelException"/> </exception>
    internal async ValueTask<Message> Request(Message message, int[] expectedResponseCodes, TimeSpan? timeout = null)
    {
        ThrowIfClosed();

        TaskCompletionSource<Message> mre = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // Add TaskCompletionSource to the dictionary it will be used to set the result of the request
        _requests.TryAdd(message.Properties.MessageId, mre);

        using var cts =
            new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(1000)); // TODO: make the timeout configurable

        using CancellationTokenRegistration ctsr = cts.Token.Register(RequestTimeoutAction);

        await InternalSendAsync(message)
            .ConfigureAwait(false);

        // The response is handled in a separate thread, see ProcessResponses method in the Init method
        Message result = await mre.Task.WaitAsync(cts.Token)
            .ConfigureAwait(false);

        // Check the responses and throw exceptions if needed.
        CheckResponse(message, expectedResponseCodes, result);

        return result;

        void RequestTimeoutAction()
        {
            Trace.WriteLine(TraceLevel.Warning, $"Request timeout for {message.Properties.MessageId}");
            if (_requests.TryRemove(message.Properties.MessageId, out TaskCompletionSource<Message>? timedOutMre))
            {
                timedOutMre.TrySetCanceled();
            }
        }
    }

    /// <summary>
    /// Check the response of a request and throw exceptions if needed
    /// </summary>
    /// <param name="sentMessage">The message sent </param>
    /// <param name="expectedResponseCodes"> The expected response codes  </param>
    /// <param name="receivedMessage">The message received from the server</param>
    /// <exception cref="ModelException"></exception>
    /// <exception cref="PreconditionFailedException"></exception>
    /// <exception cref="InvalidCodeException"></exception>
    internal void CheckResponse(Message sentMessage, int[] expectedResponseCodes, Message receivedMessage)
    {
        // Check if the response code is a number
        // by protocol the response code is in the Subject property
        if (!int.TryParse(receivedMessage.Properties.Subject, out int responseCode))
        {
            throw new ModelException($"Response code is not a number {receivedMessage.Properties.Subject}");
        }

        switch (responseCode)
        {
            case Code409:
                throw new PreconditionFailedException($"Precondition Fail. Message: {receivedMessage.Body}");
        }

        // Check if the correlationId is the same as the messageId
        if (sentMessage.Properties.MessageId != receivedMessage.Properties.CorrelationId)
        {
            throw new ModelException(
                $"CorrelationId does not match, expected {sentMessage.Properties.MessageId} but got {receivedMessage.Properties.CorrelationId}");
        }


        bool any = expectedResponseCodes.Any(c => c == responseCode);
        if (!any)
        {
            throw new InvalidCodeException(
                $"Unexpected response code: {responseCode} instead of {expectedResponseCodes.Aggregate("", (s, i) => s + i + ", ")}"
            );
        }
    }

    protected virtual async Task InternalSendAsync(Message message)
    {
        await _senderLink!.SendAsync(message)
            .ConfigureAwait(false);
    }

    public override async Task CloseAsync()
    {
        State = State.Closed;
        if (_managementSession is { IsClosed: false })
        {
            await _managementSession.CloseAsync()
                .ConfigureAwait(false);
        }
    }
}

public class InvalidCodeException(string message) : Exception(message);
