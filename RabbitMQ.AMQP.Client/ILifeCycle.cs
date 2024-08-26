// This source code is dual-licensed under the Apache License, version
// 2.0, and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2023 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

namespace RabbitMQ.AMQP.Client;

public enum State
{
    // Opening,
    Open,
    Reconnecting,
    Closing,
    Closed,
}

public class Error(string? errorCode, string? description)
{
    public string? Description { get; } = description;
    public string? ErrorCode { get; } = errorCode;

    public override string ToString()
    {
        return $"Code: {ErrorCode} - Description: {Description}";
    }
}

public delegate void LifeCycleCallBack(object sender, State previousState, State currentState, Error? failureCause);

public interface ILifeCycle : IDisposable
{
    Task CloseAsync();

    State State { get; }

    event LifeCycleCallBack ChangeState;
}
