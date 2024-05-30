using System.Collections.Concurrent;
using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client;



public class AmqpManagement : IManagement
{
    private readonly ConcurrentDictionary<string, TaskCompletionSource<Message>> _requests = new();

    // private static readonly long IdSequence = 0;
    //
    private const string ManagementNodeAddress = "/management";

    // private static readonly string REPLY_TO = "$me";
    //
    // private static readonly string GET = "GET";
    // private static readonly string POST = "POST";
    // private static readonly string PUT = "PUT";
    // private static readonly string DELETE = "DELETE";
    internal const int Code200 = 200;
    internal const string Put = "PUT";

    internal const string ReplyTo = "$me";
    // private static readonly int CODE_201 = 201;
    // private static readonly int CODE_204 = 204;
    // private static readonly int CODE_409 = 409;


    public ManagementStatus Status { get; private set; } = ManagementStatus.Closed;


    public IQueueSpecification Queue()
    {
        return new AmqpQueueSpecification(this);
    }

    public IQueueSpecification Queue(string name)
    {
        return Queue().Name(name);
    }

    private Session? _managementSession;
    private Connection? _nativeConnection;
    private SenderLink? _senderLink;

    internal void SetOpen()
    {
        Status = ManagementStatus.Open;
    }

    internal void Init(Connection connection)
    {
        _nativeConnection = connection;
        _managementSession = new Session(connection);

        var senderAttach = new Attach
        {
            SndSettleMode = SenderSettleMode.Settled,
            RcvSettleMode = ReceiverSettleMode.First,

            Properties = new Fields
            {
                { new Symbol("paired"), true }
            },
            LinkName = "management-link-pair",
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

        var receiveAttach = new Attach()
        {
            SndSettleMode = SenderSettleMode.Settled,
            RcvSettleMode = ReceiverSettleMode.First,
            Properties = new Fields
            {
                { new Symbol("paired"), true }
            },
            LinkName = "management-link-pair",
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

        _senderLink = new SenderLink(
            _managementSession, "management-link-pair", senderAttach, null);

        Thread.Sleep(500);
        var receiver = new ReceiverLink(
            _managementSession, "management-link-pair", receiveAttach, null);
        receiver.SetCredit(100);
        SetOpen();

        _ = Task.Run(async () =>
        {
            while (Status == ManagementStatus.Open)
            {
                var msg = await receiver.ReceiveAsync();
                if (msg == null)
                {
                    System.Diagnostics.Debug.WriteLine("Received null message");
                    continue;
                }

                receiver.Accept(msg);
                HandleResponseMessage(msg);
            }
        });
    }

    protected void HandleResponseMessage(Message msg)
    {
        if (msg.Properties.CorrelationId != null &&
            _requests.TryRemove(msg.Properties.CorrelationId, out var mre))
        {
            mre.SetResult(msg);
        }
        else
        {
            System.Diagnostics.Debug.WriteLine("Received unexpected message");
        }
    }

    internal async ValueTask<int> Request(object body, string path, string method,
        int[] expectedResponseCodes, TimeSpan? timeout = null)
    {
        var id = Guid.NewGuid().ToString();
        return await Request(id, body, path, method, expectedResponseCodes, timeout);
    }

    internal async ValueTask<int> Request(string id, object body, string path, string method,
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

    internal async ValueTask<int> Request(Message message, int[] expectedResponseCodes, TimeSpan? timeout = null)
    {
        if (Status != ManagementStatus.Open)
        {
            throw new ManagementClosedException("Management is not open");
        }

        TaskCompletionSource<Message> mre = new(false);
        _requests.TryAdd(message.Properties.MessageId, mre);

        using var cts = new CancellationTokenSource(timeout ?? TimeSpan.FromSeconds(5));
        cts.Token.Register(() => { _requests.TryRemove(message.Properties.MessageId, out _); }
        );

        await InternalSendAsync(message);
        var result = await mre.Task.WaitAsync(cts.Token);

        if (!int.TryParse(result.Properties.Subject, out var responseCode))
            throw new InvalidOperationException("Response code is not a number");

        var any = expectedResponseCodes.Any(c => c == responseCode);

        if (!any)
        {
            throw new InvalidCodeException(
                $"Expected response codes: {expectedResponseCodes}, got: {responseCode}");
        }

        return responseCode;
    }

    protected virtual async Task InternalSendAsync(Message message)
    {
        await _senderLink!.SendAsync(message);
    }

    public async Task CloseAsync()
    {
        Status = ManagementStatus.Closed;
        if (_nativeConnection != null)
        {
            await _nativeConnection.CloseAsync();
        }
    }
}



public class InvalidCodeException(string message) : Exception(message);
public class ManagementClosedException(string message) : Exception(message);