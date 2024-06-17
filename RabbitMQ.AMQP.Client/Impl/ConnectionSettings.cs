using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class ConnectionSettingBuilder
{
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
        return
            $"Address{{host='{Address.Host}', port={Address.Port}, path='{Address.Path}', username='{Address.User}', password='{Address.Password}'}}";
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

public class RecoveryConfiguration : IRecoveryConfiguration
{
    public static RecoveryConfiguration Create()
    {
        return new RecoveryConfiguration();
    }

    private RecoveryConfiguration()
    {
    }

    private bool _active = true;
    private bool _topology = false;

    public IRecoveryConfiguration Activated(bool activated)
    {
        _active = activated;
        return this;
    }

    public bool IsActivate()
    {
        return _active;
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
}