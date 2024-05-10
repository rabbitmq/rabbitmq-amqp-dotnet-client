namespace RabbitMQ.AMQP.Client;


// <summary>
// Represents a network address.
// </summary>
public class Address(string host, int port)
{
    public string Host { get; } = host;
    public int Port { get; } = port;

    public override string ToString()
    {
        return $"Address{{host='{Host}', port={Port}}}";
    }

    public override bool Equals(object? o)
    {
        if (this == o) return true;
        if (o == null || GetType() != o.GetType()) return false;
        var address = (Address)o;
        return Port == address.Port && Host.Equals(address.Host);
    }

    public override int GetHashCode()
    {
        return HashCode.Combine(Host, Port);
    }
}