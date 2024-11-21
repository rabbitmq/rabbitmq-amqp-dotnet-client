// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.AMQP.Client
{
    /// <summary>
    /// Interface for the recovery configuration.
    /// </summary>
    public interface IRecoveryConfiguration
    {
        /// <summary>
        /// Define if the recovery is activated.
        /// If is not activated the connection will not try to reconnect.
        /// </summary>
        /// <param name="activated"></param>
        /// <returns></returns>
        IRecoveryConfiguration Activated(bool activated);

        bool IsActivated();

        /// <summary>
        /// Define the backoff delay policy.
        /// It is used when the connection is trying to reconnect.
        /// </summary>
        /// <param name="backOffDelayPolicy"></param>
        /// <returns></returns>
        IRecoveryConfiguration BackOffDelayPolicy(IBackOffDelayPolicy backOffDelayPolicy);

        IBackOffDelayPolicy GetBackOffDelayPolicy();

        /// <summary>
        /// Define if the recovery of the topology is activated.
        /// When Activated the connection will try to recover the topology after a reconnection.
        /// It is valid only if the recovery is activated.
        /// </summary>
        /// <param name="activated"></param>
        /// <returns></returns>
        IRecoveryConfiguration Topology(bool activated);

        bool IsTopologyActive();
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
        // Activate the reconnect mechanism
        private bool _active = true;

        // Activate the recovery of the topology
        private bool _topology = false;

        private IBackOffDelayPolicy _backOffDelayPolicy = new BackOffDelayPolicy();

        /// <summary>
        /// Define if the recovery is activated.
        /// If is not activated the connection will not try to reconnect.
        /// </summary>
        /// <param name="activated"></param>
        /// <returns></returns>
        public IRecoveryConfiguration Activated(bool activated)
        {
            _active = activated;
            return this;
        }

        public bool IsActivated()
        {
            return _active;
        }

        /// <summary>
        /// Define the backoff delay policy.
        /// It is used when the connection is trying to reconnect.
        /// </summary>
        /// <param name="backOffDelayPolicy"></param>
        /// <returns></returns>
        public IRecoveryConfiguration BackOffDelayPolicy(IBackOffDelayPolicy backOffDelayPolicy)
        {
            _backOffDelayPolicy = backOffDelayPolicy;
            return this;
        }

        public IBackOffDelayPolicy GetBackOffDelayPolicy()
        {
            return _backOffDelayPolicy;
        }

        /// <summary>
        /// Define if the recovery of the topology is activated.
        /// When Activated the connection will try to recover the topology after a reconnection.
        /// It is valid only if the recovery is activated.
        /// </summary>
        /// <param name="activated"></param>
        /// <returns></returns>
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
}
