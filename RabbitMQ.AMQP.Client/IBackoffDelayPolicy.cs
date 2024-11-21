// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.AMQP.Client
{
    /// <summary>
    /// Interface for the backoff delay policy.
    /// Used during the recovery of the connection.
    /// </summary>
    public interface IBackOffDelayPolicy
    {
        /// <summary>
        /// Get the next delay in milliseconds.
        /// </summary>
        /// <returns></returns>
        int Delay();

        /// <summary>
        ///  Reset the backoff delay policy.
        /// </summary>
        void Reset();

        /// <summary>
        /// Define if the backoff delay policy is active.
        /// Can be used to disable the backoff delay policy after a certain number of retries.
        /// or when the user wants to disable the backoff delay policy.
        /// </summary>
        bool IsActive();

        int CurrentAttempt { get; }
    }

    /// <summary>
    /// Class for the backoff delay policy.
    /// Used during the recovery of the connection.
    /// </summary>
    public class BackOffDelayPolicy : IBackOffDelayPolicy
    {
        private const int StartRandomMilliseconds = 500;
        private const int EndRandomMilliseconds = 1500;

        private int _attempt = 1;
        private readonly int _maxAttempt = 12;

        public BackOffDelayPolicy()
        {
        }

        public BackOffDelayPolicy(int maxAttempt)
        {
            _maxAttempt = maxAttempt;
        }

        /// <summary>
        /// Get the next delay in milliseconds.
        /// </summary>
        /// <returns></returns>
        public int Delay()
        {
            _attempt++;
            CurrentAttempt++;
            ResetAfterMaxAttempt();
            return Utils.RandomNext(StartRandomMilliseconds, EndRandomMilliseconds) * _attempt;
        }

        /// <summary>
        ///  Reset the backoff delay policy.
        /// </summary>
        public void Reset()
        {
            _attempt = 1;
            CurrentAttempt = 0;
        }

        /// <summary>
        /// Define if the backoff delay policy is active.
        /// Can be used to disable the backoff delay policy after a certain number of retries.
        /// or when the user wants to disable the backoff delay policy.
        /// </summary>
        public bool IsActive()
        {
            return CurrentAttempt < _maxAttempt;
        }

        public int CurrentAttempt { get; private set; } = 0;

        public override string ToString()
        {
            return $"BackOffDelayPolicy{{ Attempt={_attempt}, TotalAttempt={CurrentAttempt}, IsActive={IsActive()} }}";
        }

        private void ResetAfterMaxAttempt()
        {
            if (_attempt > 5)
            {
                _attempt = 1;
            }
        }
    }
}
