namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpManagementParameters(AmqpConnection connection)
{
    private RecordingTopologyListener _topologyListener = null!;

    public AmqpManagementParameters TopologyListener(RecordingTopologyListener topologyListener)
    {
        _topologyListener = topologyListener;
        return this;
    }


    public AmqpConnection Connection()
    {
        return connection;
    }

    public RecordingTopologyListener TopologyListener()
    {
        return _topologyListener;
    }
}
