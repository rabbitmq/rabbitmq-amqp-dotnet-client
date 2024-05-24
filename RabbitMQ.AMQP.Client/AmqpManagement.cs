namespace RabbitMQ.AMQP.Client;

public class AmqpManagement : IManagement
{
    // private static readonly long ID_SEQUENCE = 0;
    //
    // private static readonly string MANAGEMENT_NODE_ADDRESS = "/management";
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


    public IQueueSpecification Queue()
    {
        throw new NotImplementedException();
    }

    public IQueueSpecification Queue(string name)
    {
        throw new NotImplementedException();
    }
}