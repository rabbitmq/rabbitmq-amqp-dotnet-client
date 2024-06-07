using Amqp;

namespace RabbitMQ.AMQP.Client.Impl;

public class AmqpAddressBuilder
{
    private string _host = "localhost";
    private int _port = 5672;
    private string _user = "guest";
    private string _password = "guest";
    private string _scheme = "AMQP";
    private string _connection = "AMQP.NET";
    private string _virtualHost = "/";


    public AmqpAddressBuilder Host(string host)
    {
        _host = host;
        return this;
    }

    public AmqpAddressBuilder Port(int port)
    {
        _port = port;
        return this;
    }

    public AmqpAddressBuilder User(string user)
    {
        _user = user;
        return this;
    }

    public AmqpAddressBuilder Password(string password)
    {
        _password = password;
        return this;
    }


    public AmqpAddressBuilder Scheme(string scheme)
    {
        _scheme = scheme;
        return this;
    }

    public AmqpAddressBuilder ConnectionName(string connection)
    {
        _connection = connection;
        return this;
    }

    public AmqpAddressBuilder VirtualHost(string virtualHost)
    {
        _virtualHost = virtualHost;
        return this;
    }

    public AmqpAddress Build()
    {
        return new AmqpAddress(_host, _port, _user,
            _password, _virtualHost,
            _scheme, _connection);
    }
}

// <summary>
// Represents a network address.
// </summary>
public class AmqpAddress : IAddress
{
    internal Address Address { get; }

    private readonly string _connectionName = "AMQP.NET";
    private readonly string _virtualHost = "/";


    public AmqpAddress(string address)
    {
        Address = new Address(address);
    }

    public AmqpAddress(string host, int port,
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

        var address = (AmqpAddress)obj;
        return Address.Host == address.Address.Host &&
               Address.Port == address.Address.Port &&
               Address.Path == address.Address.Path &&
               Address.User == address.Address.User &&
               Address.Password == address.Address.Password &&
               Address.Scheme == address.Address.Scheme;
    }

    protected bool Equals(AmqpAddress other)
    {
        return Address.Equals(other.Address);
    }

    public override int GetHashCode()
    {
        return Address.GetHashCode();
    }
}