using System.Collections.Concurrent;
using System.Collections.ObjectModel;

namespace RabbitMQ.AMQP.Client.Impl;

public class Environment() : IEnvironment
{
    private IConnectionSettings? ConnectionSettings { get; }
    private long _sequentialId = 0;
    private readonly ConcurrentDictionary<long, IConnection> _connections = [];

    private Environment(IConnectionSettings connectionSettings) : this()
    {
        ConnectionSettings = connectionSettings;
    }

    public static async Task<IEnvironment> CreateAsync(IConnectionSettings connectionSettings)
    {
        var env = new Environment(connectionSettings);
        await env.CreateConnectionAsync().ConfigureAwait(false);
        return env;
    }

    public async Task<IConnection> CreateConnectionAsync()
    {
        if (ConnectionSettings == null)
        {
            throw new ConnectionException("Connection settings are not set");
        }

        IConnection c = await AmqpConnection.CreateAsync(ConnectionSettings).ConfigureAwait(false);
        c.Id = Interlocked.Increment(ref _sequentialId);
        c.ChangeState += (sender, previousState, currentState, failureCause) =>
        {
            if (currentState != State.Closed)
            {
                return;
            }

            if (sender is IConnection connection)
            {
                _connections.TryRemove(connection.Id, out _);
            }
        };
        return c;

    }

    public ReadOnlyCollection<IConnection> GetConnections() =>
        new ReadOnlyCollection<IConnection>(_connections.Values.ToList());

    public Task CloseAsync() => Task.WhenAll(_connections.Values.Select(c => c.CloseAsync()));
}
