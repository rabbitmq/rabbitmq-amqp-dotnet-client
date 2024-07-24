﻿namespace RabbitMQ.AMQP.Client
{
    public class SaslMechanism : IEquatable<SaslMechanism>
    {
        public static readonly SaslMechanism Plain = new("PLAIN");
        public static readonly SaslMechanism External = new("EXTERNAL");

        private readonly string _saslMechanism;

        private SaslMechanism(string saslMechanism)
        {
            _saslMechanism = saslMechanism;
        }

        public string Mechanism => _saslMechanism;

        public override bool Equals(object? obj)
        {
            if (obj is null)
            {
                return false;
            }

            if (obj is not SaslMechanism)
            {
                return false;
            }

            if (Object.ReferenceEquals(this, obj))
            {
                return true;
            }

            return GetHashCode() == obj.GetHashCode();
        }

        public bool Equals(SaslMechanism? other)
        {
            if (other is null)
            {
                return false;
            }

            if (Object.ReferenceEquals(this, other))
            {
                return true;
            }

            return GetHashCode() == other.GetHashCode();
        }

        public override int GetHashCode()
        {
            return _saslMechanism.GetHashCode();
        }
    }
}
