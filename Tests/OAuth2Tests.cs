// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Linq;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Amqp;
using Microsoft.IdentityModel.Tokens;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;
using Xunit.Abstractions;
using IConnection = RabbitMQ.AMQP.Client.IConnection;

namespace Tests
{
    public class OAuth2Tests(ITestOutputHelper testOutputHelper)
        : IntegrationTest(testOutputHelper, setupConnectionAndManagement: false)
    {
        private const string Base64Key = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGH";

        private const string Audience = "rabbitmq";

        // 
        [SkippableFact]
        public async Task ConnectToRabbitMqWithOAuth2TokenShouldSuccess()
        {
            Skip.IfNot(IsCluster);
            IConnection connection = await AmqpConnection.CreateAsync(
                ConnectionSettingsBuilder.Create()
                    .Host("localhost")
                    .Port(5672)
                    .OAuth2Options(new OAuth2Options(GenerateToken(DateTime.UtcNow.AddMinutes(5))))
                    .Build());

            Assert.NotNull(connection);
            await connection.CloseAsync();
        }

        [SkippableFact]
        public async Task RefreshTokenShouldNotDisconnectTheClient()
        {
            Skip.IfNot(IsCluster);
            IConnection connection = await AmqpConnection.CreateAsync(
                ConnectionSettingsBuilder.Create()
                    .Host("localhost")
                    .Port(5672)
                    .OAuth2Options(new OAuth2Options(GenerateToken(DateTime.UtcNow.AddMilliseconds(1_000))))
                    .Build());
            await connection.RefreshTokenAsync(GenerateToken(DateTime.UtcNow.AddMinutes(5)));
            Thread.Sleep(TimeSpan.FromSeconds(1));
            Assert.NotNull(connection);
            Assert.Equal(State.Open, connection.State);
            await connection.CloseAsync();
        }

        [SkippableFact]
        public async Task ConnectToRabbitMqWithOAuth2TokenShouldDisconnectAfterTimeout()
        {
            Skip.IfNot(IsCluster);
            var l = new List<ConnectionSettingsBuilder>
            {
                ConnectionSettingsBuilder.Create().Uris(new List<Uri> { new("amqp://") }),
                ConnectionSettingsBuilder.Create().Uri(new Uri("amqp://localhost:5672")),
                ConnectionSettingsBuilder.Create().Host("localhost").Port(5672)
            };

            foreach (ConnectionSettingsBuilder builder in l)
            {
                IConnection connection = await AmqpConnection.CreateAsync(builder
                    .RecoveryConfiguration(new RecoveryConfiguration().Activated(false).Topology(false))
                    .OAuth2Options(new OAuth2Options(GenerateToken(DateTime.UtcNow.AddMilliseconds(1_000)))).Build());
                Assert.NotNull(connection);
                Assert.Equal(State.Open, connection.State);
                State? stateFrom = null;
                State? stateTo = null;
                Error? stateError = null;
                TaskCompletionSource<bool> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
                connection.ChangeState += (_, from, to, error) =>
                {
                    stateFrom = from;
                    stateTo = to;
                    stateError = error;
                    tcs.SetResult(true);
                };

                await tcs.Task;
                Assert.NotNull(stateFrom);
                Assert.NotNull(stateTo);
                Assert.NotNull(stateError);
                Assert.NotNull(stateError.ErrorCode);
                Assert.Equal(State.Open, stateFrom);
                Assert.Equal(State.Closed, stateTo);
                Assert.Equal(State.Closed, connection.State);
                Assert.Contains(stateError.ErrorCode, "amqp:unauthorized-access");
            }
        }

        private static string GenerateToken(DateTime duration)
        {
            byte[] decodedKey = Convert.FromBase64String(Base64Key);

            var claims = new[]
            {
                new Claim(JwtRegisteredClaimNames.Iss, "unit_test"),
                new Claim(JwtRegisteredClaimNames.Aud, Audience),
                new Claim(JwtRegisteredClaimNames.Exp, new DateTimeOffset(duration).ToUnixTimeSeconds().ToString()),
                new Claim("scope", "rabbitmq.configure:*/* rabbitmq.write:*/* rabbitmq.read:*/*"),
                new Claim("random", GenerateRandomString(6))
            };

            var key = new SymmetricSecurityKey(decodedKey);
            var creds = new SigningCredentials(key, SecurityAlgorithms.HmacSha256);

            var token = new JwtSecurityToken(
                claims: claims,
                expires: duration,
                signingCredentials: creds
            )
            { Header = { ["kid"] = "token-key" } };

            var tokenHandler = new JwtSecurityTokenHandler();
            return tokenHandler.WriteToken(token);
        }

        private static string GenerateRandomString(int length)
        {
            const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            var random = new Random();
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[random.Next(s.Length)]).ToArray());
        }
    }
}
