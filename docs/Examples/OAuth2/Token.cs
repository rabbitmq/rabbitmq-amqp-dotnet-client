// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Text;
using Microsoft.IdentityModel.Tokens;

namespace OAuth2
{
    public class Token
    {
        private const string Base64Key = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGH";

        private const string Audience = "rabbitmq";

        public static string GenerateToken(DateTime duration)
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
            );

            token.Header["kid"] = "token-key";

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
