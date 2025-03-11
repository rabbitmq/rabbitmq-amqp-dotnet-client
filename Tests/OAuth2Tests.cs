// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System;
using System.Collections.Generic;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Tokens;
using RabbitMQ.AMQP.Client;
using RabbitMQ.AMQP.Client.Impl;
using Xunit;

namespace Tests
{
    public class OAuth2Tests
    {
        private const string Base64Key = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGH";

        private const string Audience = "rabbitmq";

        [Fact]
        public async Task ConnectToRabbitMqWithOAuth2Token()
        {
            IConnection connection = await AmqpConnection.CreateAsync(
                ConnectionSettingsBuilder.Create()
                    .Host("localhost")
                    .Port(5672)
                    .OAuth2Options(new OAuth2Options(GenerateToken(DateTime.UtcNow.AddMinutes(5))))
                    .Build());

            Assert.NotNull(connection);
            await connection.CloseAsync();
        }

        private static string GenerateToken(DateTime expiration)
        {
            byte[] decodedKey = Convert.FromBase64String(Base64Key);

            var claims = new List<Claim>
            {
                new (JwtRegisteredClaimNames.Iss, "unit_test"),
                new (JwtRegisteredClaimNames.Aud, Audience),
                new (JwtRegisteredClaimNames.Exp,
                    new DateTimeOffset(expiration).ToUniversalTime().ToUnixTimeSeconds().ToString()),
                new ("scope", "rabbitmq.configure:*/*"),
                new ("scope", "rabbitmq.write:*/*"),
                new ("scope", "rabbitmq.read:*/*"),
                new ("random", RandomString(6))
            };

            var tokenHandler = new JwtSecurityTokenHandler();
            var claimIdentity = new ClaimsIdentity(claims);
            var tokenDescriptor = new SecurityTokenDescriptor
            {
                Subject = claimIdentity,
                Expires = expiration,
                SigningCredentials =
                    new SigningCredentials(new SymmetricSecurityKey(decodedKey),
                        SecurityAlgorithms.HmacSha256Signature),
                Claims = new Dictionary<string, object> { ["kid"] = "token-key" }
            };

            var token = tokenHandler.CreateToken(tokenDescriptor);
            return tokenHandler.WriteToken(token);
        }

        private static string RandomString(int length)
        {
            const string chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            var result = new StringBuilder(length);

            using (var rng = RandomNumberGenerator.Create())
            {
                byte[] byteArray = new byte[length];
                rng.GetBytes(byteArray);

                foreach (byte byteValue in byteArray)
                {
                    result.Append(chars[byteValue % chars.Length]);
                }
            }

            return result.ToString();
        }
    }
}
