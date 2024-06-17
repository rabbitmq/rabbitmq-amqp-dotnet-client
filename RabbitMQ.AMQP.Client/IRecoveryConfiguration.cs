namespace RabbitMQ.AMQP.Client;

public interface IRecoveryConfiguration
{
    IRecoveryConfiguration Activated(bool activated);
    
    bool IsActivate();

    IRecoveryConfiguration BackOffDelayPolicy(IBackOffDelayPolicy backOffDelayPolicy);

    IRecoveryConfiguration Topology(bool activated);
    
    bool IsTopologyActive();

}


public interface IBackOffDelayPolicy 
{
    int Next();
    void Reset();
}