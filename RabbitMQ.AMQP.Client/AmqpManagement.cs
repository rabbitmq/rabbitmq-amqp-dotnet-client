using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client;

public enum AmqpManagementStatus
{
    Closed,
    Initializing,
    Open,
}

public class AmqpManagement : IManagement
{
    // private static readonly long ID_SEQUENCE = 0;
    //
    private const string ManagementNodeAddress = "/management";

    // private static readonly string REPLY_TO = "$me";
    //
    // private static readonly string GET = "GET";
    // private static readonly string POST = "POST";
    // private static readonly string PUT = "PUT";
    // private static readonly string DELETE = "DELETE";
    // private static readonly int CODE_200 = 200;
    // private static readonly int CODE_201 = 201;
    // private static readonly int CODE_204 = 204;
    // private static readonly int CODE_409 = 409;


    public AmqpManagementStatus Status { get; private set; } = AmqpManagementStatus.Closed;


    public IQueueSpecification Queue()
    {
        return new AmqpQueueSpecification(this);
    }

    public IQueueSpecification Queue(string name)
    {
        return this.Queue().Name(name);
    }

    private Session? _managementSession;
    private Connection? _nativeConnection;
    private SenderLink? _senderLink;

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
        Status = AmqpManagementStatus.Open;

        _ = Task.Run(async () =>
        {
            while (Status == AmqpManagementStatus.Open)
            {
                var msg = await receiver.ReceiveAsync();
                if (msg == null)
                {
                    System.Diagnostics.Debug.WriteLine("Received null message");
                    continue;
                }

                receiver.Accept(msg);
            }
        });
    }

    public Task SendAsync(Message message)
    {
        if (Status != AmqpManagementStatus.Open)
        {
            throw new InvalidOperationException("Management is not open");
        }

        return _senderLink!.SendAsync(message);
    }

    public async Task CloseAsync()
    {
        Status = AmqpManagementStatus.Closed;
        if (_nativeConnection != null)
        {
            await _nativeConnection.CloseAsync();
        }
    }
}