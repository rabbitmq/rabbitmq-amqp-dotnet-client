namespace RabbitMQ.AMQP.Client;

public interface IRecoveryConfiguration
{
    IRecoveryConfiguration Activated(bool activated);
    
    bool IsActivate();

    // IRecoveryConfiguration BackOffDelayPolicy(BackOffDelayPolicy backOffDelayPolicy);

    IRecoveryConfiguration Topology(bool activated);
    
    bool IsTopologyActive();

}