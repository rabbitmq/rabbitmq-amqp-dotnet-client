using System.Collections.Concurrent;
using System.Diagnostics;
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
public class AmqpManagement(AmqpManagementParameters parameters) : AbstractLifeCycle, IManagement
{
    // The requests are stored in a dictionary with the correlationId as the key
    // The correlationId is used to match the request with the response
    private readonly ConcurrentDictionary<string, TaskCompletionSource<Message>> _requests = new();

    // private static readonly long IdSequence = 0;
    private const string ManagementNodeAddress = "/management";
    private const string LinkPairName = "management-link-pair";

    private Session? _managementSession;
    private SenderLink? _senderLink;
    private ReceiverLink? _receiverLink;

    internal const int Code200 = 200;
    internal const int Code201 = 201;
    internal const int Code204 = 204;
    internal const int Code409 = 409;
    internal const string Put = "PUT";
    internal const string Get = "GET";
    internal const string Post = "POST";
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

    public async Task<IQueueInfo> GetQueueInfoAsync(string queueName,
        CancellationToken cancellationToken = default)
    {
        // TODO: validate queueName?
        // TODO: encodePathSegment(queues)
        Message response = await RequestAsync($"/{Consts.Queues}/{Utils.EncodePathSegment(queueName)}",
            AmqpManagement.Get,
            [
                AmqpManagement.Code200
            ], null, cancellationToken).ConfigureAwait(false);

        return new DefaultQueueInfo((Map)response.Body);
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

    public IBindingSpecification Binding()
    {
        return new AmqpBindingSpecification(this);
    }

    public ITopologyListener TopologyListener()
    {
        return parameters.TopologyListener();
    }

    public override async Task OpenAsync()
    {
        if (State == State.Open)
        {
            return;
        }

        if (_managementSession == null || _managementSession.IsClosed)
        {
            _managementSession = new Session(parameters.Connection().NativeConnection());
        }

        await EnsureSenderLinkAsync()
            .ConfigureAwait(false);

        await EnsureReceiverLinkAsync()
            .ConfigureAwait(false);

        // TODO do something with this task?
        _ = Task.Run(ProcessResponses);

        _managementSession.Closed += (sender, error) =>
        {
            if (State != State.Closed)
            {
                Trace.WriteLine(TraceLevel.Warning, $"Management session closed " +
                                                    $"with error: {Utils.ConvertError(error)} " +
                                                    $" AmqpManagement: {ToString()}");
            }

            OnNewStatus(State.Closed, Utils.ConvertError(error));
            ConnectionCloseTaskCompletionSource.TrySetResult(true);
        };

        await base.OpenAsync()
            .ConfigureAwait(false);
    }

    private async Task ProcessResponses()
    {
        try
        {
            while (_managementSession?.IsClosed == false &&
                   parameters.Connection().NativeConnection()!.IsClosed == false)
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
            if (_receiverLink?.IsClosed == false)
            {
                Trace.WriteLine(TraceLevel.Error,
                    $"Receiver link error in management session {e}. Receiver link closed: {_receiverLink?.IsClosed}");
            }
        }

        Trace.WriteLine(TraceLevel.Verbose, "ProcessResponses Task closed");
    }


    private async Task EnsureReceiverLinkAsync()
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

            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _receiverLink = new ReceiverLink(
                _managementSession, LinkPairName, receiveAttach, (ILink link, Attach attach) =>
                {
                    Debug.Assert(Object.ReferenceEquals(_receiverLink, link));
                    tcs.SetResult();
                });

            await tcs.Task
                .ConfigureAwait(false);

            _receiverLink.SetCredit(1);
        }
    }


    private Task EnsureSenderLinkAsync()
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

            var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            _senderLink = new SenderLink(
                _managementSession, LinkPairName, senderAttach, (ILink link, Attach attach) =>
                {
                    Debug.Assert(Object.ReferenceEquals(_senderLink, link));
                    tcs.SetResult();
                });
            return tcs.Task;
        }
        else
        {
            return Task.CompletedTask;
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

    internal ValueTask<Message> RequestAsync(string path, string method,
        int[] expectedResponseCodes,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        return RequestAsync(null, path, method, expectedResponseCodes,
            timeout, cancellationToken);
    }

    internal ValueTask<Message> RequestAsync(object? body, string path, string method,
        int[] expectedResponseCodes,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        string id = Guid.NewGuid().ToString();
        return RequestAsync(id, body, path, method, expectedResponseCodes, timeout);
    }

    internal ValueTask<Message> RequestAsync(string id, object? body, string path, string method,
        int[] expectedResponseCodes,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default)
    {
        var message = new Message(body)
        {
            Properties = new Properties
            {
                MessageId = id,
                To = path,
                Subject = method,
                ReplyTo = ReplyTo
            }
        };
        return RequestAsync(message, expectedResponseCodes, timeout, cancellationToken);
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
    /// <param name="expectedResponseCodes">The response codes expected for a specific call. See Code* Constants </param>
    /// <param name="argTimeout">Default timeout for a request </param>
    /// <param name="cancellationToken">Cancellation token for this request</param>
    /// <returns> A message with the Info response. For example in case of Queue creation is DefaultQueueInfo </returns>
    /// <exception cref="ModelException"> Application errors, see <see cref="ModelException"/> </exception>
    internal async ValueTask<Message> RequestAsync(Message message, int[] expectedResponseCodes,
        TimeSpan? argTimeout = null, CancellationToken cancellationToken = default)
    {
        ThrowIfClosed();

        // TODO: make the timeout configurable
        TimeSpan timeout = argTimeout ?? TimeSpan.FromSeconds(30);

        TaskCompletionSource<Message> mre = new(TaskCreationOptions.RunContinuationsAsynchronously);

        // Add TaskCompletionSource to the dictionary it will be used to set the result of the request
        _requests.TryAdd(message.Properties.MessageId, mre);

        // TODO: re-use with TryReset?
        using var timeoutCts = new CancellationTokenSource(timeout);
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, timeoutCts.Token);

        void RequestTimeoutAction()
        {
            Trace.WriteLine(TraceLevel.Warning, $"Request timeout for {message.Properties.MessageId}");

            if (_requests.TryRemove(message.Properties.MessageId, out TaskCompletionSource<Message>? timedOutMre))
            {
                if (false == timedOutMre.TrySetCanceled(linkedCts.Token))
                {
                    // TODO debug log rare condition?
                }
            }
            else
            {
                // TODO log missing request?
            }
        }

        using CancellationTokenRegistration ctsr = timeoutCts.Token.Register(RequestTimeoutAction);

        // NOTE: no cancellation token support
        await InternalSendAsync(message, timeout)
            .ConfigureAwait(false);

        // The response is handled in a separate thread, see ProcessResponses method in the Init method
        Message result = await mre.Task.WaitAsync(linkedCts.Token)
            .ConfigureAwait(false);

        // Check the responses and throw exceptions if needed.
        CheckResponse(message, expectedResponseCodes, result);

        return result;
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
                throw new PreconditionFailedException($"{receivedMessage.Body}");
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

    protected virtual async Task InternalSendAsync(Message message, TimeSpan timeout)
    {
        if (_senderLink is null)
        {
            // TODO create "internal bug" exception type?
            throw new InvalidOperationException("_senderLink is null, report via https://github.com/rabbitmq/rabbitmq-amqp-dotnet-client/issues");
        }

        await _senderLink.SendAsync(message, timeout)
            .ConfigureAwait(false);
    }

    public override async Task CloseAsync()
    {
        if (_managementSession is { IsClosed: false })
        {
            OnNewStatus(State.Closing, null);

            await _managementSession.CloseAsync()
                .ConfigureAwait(false);

            await ConnectionCloseTaskCompletionSource.Task.WaitAsync(TimeSpan.FromSeconds(10)).ConfigureAwait(false);

            _managementSession = null;
            _senderLink = null;
            _receiverLink = null;
            // this is actually a double set of the status, but it is needed to ensure that the status is set to closed
            // but the `OnNewStatus` is idempotent
            OnNewStatus(State.Closed, null);
        }
    }

    public override string ToString()
    {
        string info = $"AmqpManagement{{" +
                      $"AmqpConnection='{parameters.Connection()}', " +
                      $"Status='{State.ToString()}'" +
                      $"ReceiverLink closed: {_receiverLink?.IsClosed} " +
                      $"}}";

        return info;
    }


    internal void ChangeStatus(State newState, Error? error)
    {
        OnNewStatus(newState, error);
    }
}

public class InvalidCodeException(string message) : Exception(message);
