using Amqp;

namespace RabbitMQ.AMQP.Client;

public class AmqpAddressBuilder
{
    private string _host = "localhost";
    private int _port = 5672;
    private string _user = "guest";
    private string _password = "guest";
    private string _path = "/";
    private string _scheme = "AMQP";

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

    public AmqpAddressBuilder Path(string path)
    {
        _path = path;
        return this;
    }

    public AmqpAddressBuilder Scheme(string scheme)
    {
        _scheme = scheme;
        return this;
    }

    public AmqpAddress Build()
    {
        return new AmqpAddress(_host, _port, _user, _password, _path, _scheme);
    }
}

// <summary>
// Represents a network address.
// </summary>
public class AmqpAddress : IAddress
{
    internal Address Address { get; }


    public AmqpAddress(string address)
    {
        Address = new Address(address);
    }

    public AmqpAddress(string host, int port,
        string user,
        string password,
        string path, string scheme)
    {
        Address = new Address(host, port, user, password, path, scheme);
    }

    public string Host()
    {
        return Address.Host;
    }


    public int Port()
    {
        return Address.Port;
    }


    public string Path()
    {
        return Address.Path;
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