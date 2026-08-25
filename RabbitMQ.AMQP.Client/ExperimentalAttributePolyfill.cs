// This source code is dual-licensed under the Apache License, version 2.0,
// and the Mozilla Public License, version 2.0.
// Copyright (c) 2017-2024 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries.

// ExperimentalAttribute was introduced in .NET 7; provide a shim for netstandard2.0.
#if !NET7_0_OR_GREATER
namespace System.Diagnostics.CodeAnalysis
{
    [AttributeUsage(
        AttributeTargets.Assembly | AttributeTargets.Module | AttributeTargets.Class |
        AttributeTargets.Struct | AttributeTargets.Enum | AttributeTargets.Constructor |
        AttributeTargets.Method | AttributeTargets.Property | AttributeTargets.Field |
        AttributeTargets.Event | AttributeTargets.Interface | AttributeTargets.Delegate,
        Inherited = false)]
    internal sealed class ExperimentalAttribute : Attribute
    {
        public ExperimentalAttribute(string diagnosticId)
        {
            DiagnosticId = diagnosticId;
        }

        public string DiagnosticId { get; }

        public string? UrlFormat { get; set; }
    }
}
#endif
