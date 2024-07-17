using Amqp;
using Amqp.Framing;
using Amqp.Types;

namespace RabbitMQ.AMQP.Client.Impl;

public class FieldNotSetException : Exception
{
}

public class AmqpMessage : IMessage
{
    public Message NativeMessage { get; }

    public AmqpMessage()
    {
        NativeMessage = new Message();
    }


    public AmqpMessage(object body)
    {
        NativeMessage = new Message(body);
    }

    public AmqpMessage(Message nativeMessage)
    {
        NativeMessage = nativeMessage;
    }

    private void ThrowIfPropertiesNotSet()
    {
        if (NativeMessage.Properties == null)
        {
            throw new FieldNotSetException();
        }
    }

    private void EnsureProperties()
    {
        NativeMessage.Properties ??= new Properties();
    }


    private void ThrowIfAnnotationsNotSet()
    {
        if (NativeMessage.MessageAnnotations == null)
        {
            throw new FieldNotSetException();
        }
    }

    private void EnsureAnnotations()
    {
        NativeMessage.MessageAnnotations ??= new MessageAnnotations();
    }


    public object Body()
    {
        return NativeMessage.Body;
    }

    public string MessageId()
    {
        ThrowIfPropertiesNotSet();
        return NativeMessage.Properties.MessageId;
    }

    public IMessage MessageId(string id)
    {
        EnsureProperties();
        NativeMessage.Properties.MessageId = id;
        return this;
    }

    public string CorrelationId()
    {
        ThrowIfPropertiesNotSet();
        return NativeMessage.Properties.CorrelationId;
    }

    public IMessage CorrelationId(string id)
    {
        EnsureProperties();
        NativeMessage.Properties.CorrelationId = id;
        return this;
    }

    public string ReplyTo()
    {
        ThrowIfPropertiesNotSet();
        return NativeMessage.Properties.ReplyTo;
    }

    public IMessage ReplyTo(string id)
    {
        EnsureProperties();
        NativeMessage.Properties.ReplyTo = id;
        return this;
    }

    public string Subject()
    {
        ThrowIfPropertiesNotSet();
        return NativeMessage.Properties.Subject;
    }

    public IMessage Subject(string subject)
    {
        EnsureProperties();
        NativeMessage.Properties.Subject = subject;
        return this;
    }

    // Annotations

    public IMessage Annotation(string key, object value)
    {
        EnsureAnnotations();
        NativeMessage.MessageAnnotations[new Symbol(key)] = value;
        return this;
    }

    public object Annotation(string key)
    {
        ThrowIfAnnotationsNotSet();
        return NativeMessage.MessageAnnotations[key];
    }
}
