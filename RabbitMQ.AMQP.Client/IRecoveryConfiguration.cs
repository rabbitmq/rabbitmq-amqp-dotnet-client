namespace RabbitMQ.AMQP.Client;


/// <summary>
/// Interface for the recovery configuration.
/// </summary>
public interface IRecoveryConfiguration
{
    /// <summary>
    /// Define if the recovery is activated.
    /// If is not activated the connection will not try to reconnect.
    /// </summary>
    /// <param name="activated"></param>
    /// <returns></returns>
    IRecoveryConfiguration Activated(bool activated);

    bool IsActivate();

    /// <summary>
    /// Define the backoff delay policy.
    /// It is used when the connection is trying to reconnect.
    /// </summary>
    /// <param name="backOffDelayPolicy"></param>
    /// <returns></returns>
    IRecoveryConfiguration BackOffDelayPolicy(IBackOffDelayPolicy backOffDelayPolicy);

    /// <summary>
    /// Define if the recovery of the topology is activated.
    /// When Activated the connection will try to recover the topology after a reconnection.
    /// It is valid only if the recovery is activated.
    /// </summary>
    /// <param name="activated"></param>
    /// <returns></returns>
    IRecoveryConfiguration Topology(bool activated);

    bool IsTopologyActive();

}

/// <summary>
/// Interface for the backoff delay policy.
/// Used during the recovery of the connection.
/// </summary>
public interface IBackOffDelayPolicy
{
    /// <summary>
    /// Get the next delay in milliseconds.
    /// </summary>
    /// <returns></returns>
    int Delay();

    /// <summary>
    ///  Reset the backoff delay policy.
    /// </summary>
    void Reset();

    /// <summary>
    /// Define if the backoff delay policy is active.
    /// Can be used to disable the backoff delay policy after a certain number of retries.
    /// or when the user wants to disable the backoff delay policy.
    /// </summary>
    bool IsActive();
}
