﻿// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IoTSolutions.OpcTwin.EdgeService.Discovery {
    using System;
    using System.Net.Sockets;

    public interface IAsyncProbe : IDisposable {

        /// <summary>
        /// Complete probe using the passed in event arg.
        /// </summary>
        /// <param name="arg"></param>
        /// <param name="ok">
        /// If the probe returns true, this value indicates
        /// whether the port is a valid port.
        /// </param>
        /// <returns>
        /// false if expected to be called again.
        /// true if probe is complete.
        /// </returns>
        bool Complete(SocketAsyncEventArgs arg, out bool ok);

        /// <summary>
        /// Reset probe to beginning cancelling any outstanding
        /// socket operations.
        /// </summary>
        void Reset();
    }
}