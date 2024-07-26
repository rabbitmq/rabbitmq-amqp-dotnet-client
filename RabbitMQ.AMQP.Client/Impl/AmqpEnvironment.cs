using System.Collections.Concurrent;
using System.Collections.ObjectModel;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpEnvironment : IEnvironment
{
    private IConnectionSettings? ConnectionSettings { get; }
    private long _sequentialId = 0;
    private readonly ConcurrentDictionary<long, IConnection> _connections = [];

    private AmqpEnvironment(IConnectionSettings connectionSettings)
    {
        ConnectionSettings = connectionSettings;
    }

    public static Task<IEnvironment> CreateAsync(IConnectionSettings connectionSettings)
    {
        return Task.FromResult<IEnvironment>(new AmqpEnvironment(connectionSettings));
    }

    public async Task<IConnection> CreateConnectionAsync(IConnectionSettings connectionSettings)
    {
        IConnection c = await AmqpConnection.CreateAsync(connectionSettings).ConfigureAwait(false);
        c.Id = Interlocked.Increment(ref _sequentialId);
        _connections.TryAdd(c.Id, c);
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

    public async Task<IConnection> CreateConnectionAsync()
    {
        if (ConnectionSettings != null)
        {
            return await CreateConnectionAsync(ConnectionSettings).ConfigureAwait(false);
        }

        throw new ConnectionException("Connection settings are not set");
    }

    public ReadOnlyCollection<IConnection> GetConnections() =>
        new(_connections.Values.ToList());

    public Task CloseAsync() => Task.WhenAll(_connections.Values.Select(c => c.CloseAsync()));
}
