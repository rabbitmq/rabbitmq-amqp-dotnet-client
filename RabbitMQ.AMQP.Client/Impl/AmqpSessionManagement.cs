using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpSessionManagement(AmqpConnection amqpConnection, int maxSessionsPerItem)
{
    private int MaxSessionsPerItem { get; } = maxSessionsPerItem;
    private List<Session> Sessions { get; } = [];

    public Session GetOrCreateSession()
    {
        if (Sessions.Count >= MaxSessionsPerItem)
        {
            return Sessions.First();
        }

        var session = new Session(amqpConnection.NativeConnection);
        Sessions.Add(session);
        return session;
    }

    public void ClearSessions()
    {
        Sessions.Clear();
    }
}
