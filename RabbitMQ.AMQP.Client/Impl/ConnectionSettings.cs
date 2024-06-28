using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class ConnectionSettingBuilder
{
    // TODO: maybe add the event "LifeCycle" to the builder
    private string _host = "localhost";
    private int _port = 5672;
    private string _user = "guest";
    private string _password = "guest";
    private string _scheme = "AMQP";
    private string _connection = "AMQP.NET";
    private string _virtualHost = "/";
    private IRecoveryConfiguration _recoveryConfiguration = Impl.RecoveryConfiguration.Create();


    private ConnectionSettingBuilder()
    {
    }

    public static ConnectionSettingBuilder Create()
    {
        return new ConnectionSettingBuilder();
    }


    public ConnectionSettingBuilder Host(string host)
    {
        _host = host;
        return this;
    }

    public ConnectionSettingBuilder Port(int port)
    {
        _port = port;
        return this;
    }

    public ConnectionSettingBuilder User(string user)
    {
        _user = user;
        return this;
    }

    public ConnectionSettingBuilder Password(string password)
    {
        _password = password;
        return this;
    }


    public ConnectionSettingBuilder Scheme(string scheme)
    {
        _scheme = scheme;
        return this;
    }

    public ConnectionSettingBuilder ConnectionName(string connection)
    {
        _connection = connection;
        return this;
    }

    public ConnectionSettingBuilder VirtualHost(string virtualHost)
    {
        _virtualHost = virtualHost;
        return this;
    }

    public ConnectionSettingBuilder RecoveryConfiguration(IRecoveryConfiguration recoveryConfiguration)
    {
        _recoveryConfiguration = recoveryConfiguration;
        return this;
    }

    public ConnectionSettings Build()
    {
        var c = new ConnectionSettings(_host, _port, _user,
            _password, _virtualHost,
            _scheme, _connection)
        {
            RecoveryConfiguration = (RecoveryConfiguration)_recoveryConfiguration
        };

        return c;
    }
}

// <summary>
// Represents a network address.
// </summary>
public class ConnectionSettings : IConnectionSettings
{
    internal Address Address { get; }

    private readonly string _connectionName = "";
    private readonly string _virtualHost = "/";


    public ConnectionSettings(string address)
    {
        Address = new Address(address);
    }

    public ConnectionSettings(string host, int port,
        string user,
        string password,
        string virtualHost, string scheme, string connectionName)
    {
        Address = new Address(host, port, user, password, "/", scheme);
        _connectionName = connectionName;
        _virtualHost = virtualHost;
    }

    public string Host()
    {
        return Address.Host;
    }


    public int Port()
    {
        return Address.Port;
    }


    public string VirtualHost()
    {
        return _virtualHost;
    }

    public string User()
    {
        return Address.User;
    }


    public string Password()
    {
        return Address.Password;
    }


    public string Scheme()
    {
        return Address.Scheme;
    }

    public string ConnectionName()
    {
        return _connectionName;
    }

    public override string ToString()
    {
        var i =
            $"Address" +
            $"host='{Address.Host}', " +
            $"port={Address.Port}, VirtualHost='{_virtualHost}', path='{Address.Path}', " +
            $"username='{Address.User}', ConnectionName='{_connectionName}'";
        return i;
    }


    public override bool Equals(object? obj)
    {
        if (obj == null || GetType() != obj.GetType())
        {
            return false;
        }

        var address = (ConnectionSettings)obj;
        return Address.Host == address.Address.Host &&
               Address.Port == address.Address.Port &&
               Address.Path == address.Address.Path &&
               Address.User == address.Address.User &&
               Address.Password == address.Address.Password &&
               Address.Scheme == address.Address.Scheme;
    }

    protected bool Equals(ConnectionSettings other)
    {
        return Address.Equals(other.Address);
    }

    public override int GetHashCode()
    {
        return Address.GetHashCode();
    }

    public RecoveryConfiguration RecoveryConfiguration { get; set; } = RecoveryConfiguration.Create();
}

/// <summary>
/// RecoveryConfiguration is a class that represents the configuration of the recovery of the topology.
/// It is used to configure the recovery of the topology of the server after a connection is established in case of a reconnection
/// The RecoveryConfiguration can be disabled or enabled.
/// If RecoveryConfiguration._active is disabled, the reconnect mechanism will not be activated.
/// If RecoveryConfiguration._topology is disabled, the recovery of the topology will not be activated.
/// </summary>
public class RecoveryConfiguration : IRecoveryConfiguration
{
    public static RecoveryConfiguration Create()
    {
        return new RecoveryConfiguration();
    }

    private RecoveryConfiguration()
    {
    }

    // Activate the reconnect mechanism
    private bool _active = true;

    // Activate the recovery of the topology
    private bool _topology = false;

    private IBackOffDelayPolicy _backOffDelayPolicy = Impl.BackOffDelayPolicy.Create();

    public IRecoveryConfiguration Activated(bool activated)
    {
        _active = activated;
        return this;
    }

    public bool IsActivate()
    {
        return _active;
    }

    public IRecoveryConfiguration BackOffDelayPolicy(IBackOffDelayPolicy backOffDelayPolicy)
    {
        _backOffDelayPolicy = backOffDelayPolicy;
        return this;
    }

    public IBackOffDelayPolicy GetBackOffDelayPolicy()
    {
        return _backOffDelayPolicy;
    }


    public IRecoveryConfiguration Topology(bool activated)
    {
        _topology = activated;
        return this;
    }

    public bool IsTopologyActive()
    {
        return _topology;
    }

    public override string ToString()
    {
        return
            $"RecoveryConfiguration{{ Active={_active}, Topology={_topology}, BackOffDelayPolicy={_backOffDelayPolicy} }}";
    }
}

public class BackOffDelayPolicy : IBackOffDelayPolicy
{
    public static BackOffDelayPolicy Create()
    {
        return new BackOffDelayPolicy();
    }

    private BackOffDelayPolicy()
    {
    }

    private const int StartRandomMilliseconds = 500;
    private const int EndRandomMilliseconds = 1500;

    private int _attempt = 1;
    private int _totalAttempt = 0;

    private void ResetAfterMaxAttempt()
    {
        if (_attempt > 5)
            _attempt = 1;
    }

    public int Delay()
    {
        _attempt++;
        _totalAttempt++;
        ResetAfterMaxAttempt();
        return Random.Shared.Next(StartRandomMilliseconds, EndRandomMilliseconds) * _attempt;
    }

    public void Reset()
    {
        _attempt = 1;
        _totalAttempt = 0;
    }

    public bool IsActive()
    {
        return _totalAttempt < 12;
    }


    public override string ToString()
    {
        return $"BackOffDelayPolicy{{ Attempt={_attempt}, TotalAttempt={_totalAttempt}, IsActive={IsActive} }}";
    }
}