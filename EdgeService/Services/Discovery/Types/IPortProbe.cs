// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IoTSolutions.OpcTwin.EdgeService.Discovery {
    using System.Net.Sockets;
    using System.Threading;
    using System.Threading.Tasks;

    public interface IPortProbe {

        /// <summary>
        /// Check whether the port is valid
        /// </summary>
        /// <param name="arg"></param>
        /// <returns></returns>
        bool OnProbe(SocketAsyncEventArgs arg, out bool ok);
    }
}