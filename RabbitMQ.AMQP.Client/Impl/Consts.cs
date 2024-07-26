namespace RabbitMQ.AMQP.Client.Impl;

public class Consts
{
    public const string Exchanges = "exchanges";
    public const string Key = "key";
    public const string Queues = "queues";
    public const string Bindings = "bindings";

    /// <summary>
    /// <code>uint.MinValue</code> means "no limit"
    /// </summary>
    public const uint DefaultMaxFrameSize = uint.MinValue; // NOTE: Azure/amqpnetlite uses 256 * 1024
}
