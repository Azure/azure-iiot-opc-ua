// ------------------------------------------------------------
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//  Licensed under the MIT License (MIT). See License.txt in the repo root for license information.
// ------------------------------------------------------------

namespace Microsoft.Azure.IoTSolutions.OpcTwin.EdgeService.Discovery {
    using Microsoft.Azure.IoTSolutions.Common.Diagnostics;
    using Opc.Ua;
    using Opc.Ua.Bindings;
    using System;
    using System.IO;
    using System.Net;
    using System.Net.Sockets;
    using System.Text;
    using System.Threading;
    using System.Threading.Tasks;

    public class OpcUaServerProbe : IPortProbe {

        /// <summary>
        /// Create opc ua server probe
        /// </summary>
        /// <param name="logger"></param>
        public OpcUaServerProbe(ILogger logger) {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _buffer = new byte[160];
            _messageContext = new ServiceMessageContext {
                Factory = new EncodeableFactory(),
                NamespaceUris = new NamespaceTable(),
                ServerUris = new StringTable(),
            };
        }

        /// <summary>
        /// Manage probe
        /// </summary>
        /// <param name="arg"></param>
        /// <param name="ok"></param>
        /// <returns>true if completed, false to be called again</returns>
        public bool OnProbe(SocketAsyncEventArgs arg, out bool ok) {
            ok = false;
            if (arg.SocketError != SocketError.Success) {
                return true;
            }
            if (arg.ConnectSocket == null) {
                return true;
            }

            switch(_state) {
                case State.Begin:
                    // Begin
                    _socket = arg.ConnectSocket;
                    _logger.Debug($"Testing {_socket.RemoteEndPoint} ...", () => { });
                    _size = WriteRequestBuffer(_socket.RemoteEndPoint);
                    _len = 0;
                    _state = State.Write;
                    return OnProbe(arg, out ok);
                case State.Write:
                    // Send 
                    arg.SetBuffer(_buffer, _len, _size - _len);
                    if (!_socket.SendAsync(arg)) {
                        return OnProbe(arg, out ok);
                    }
                    return false;
                case State.ReadSize:
                case State.Read:
                case State.Validate:
                    return false;
            }
            return true;
        }

        /// <summary>
        /// Sends a Hello message and validates response.
        /// </summary>
        /// <param name="socket"></param>
        /// <returns></returns>
        public async Task<bool> IsValidAsync(Socket socket, CancellationToken ct) {
            _logger.Debug($"Testing {socket.RemoteEndPoint} ...", () => { });
            var size = WriteRequestBuffer(socket.RemoteEndPoint);
            try {
                for (var len = 0; len != size;) {
                    // Write message
                    len += await socket.SendAsync(
                        new ArraySegment<byte>(_buffer, len, size - len), SocketFlags.None);
                    ct.ThrowIfCancellationRequested();
                }
                size = TcpMessageLimits.MessageTypeAndSize;
                for (var len = 0; len != size;) {
                    // Read type and size first
                    len += await socket.ReceiveAsync(
                        new ArraySegment<byte>(_buffer, len, size - len), SocketFlags.None);
                    ct.ThrowIfCancellationRequested();
                }
                var type = BitConverter.ToUInt32(_buffer, 0);
                if (type != TcpMessageType.Acknowledge) {
                    return false;
                }
                size = (int)BitConverter.ToUInt32(_buffer, 4);
                if (size > _buffer.Length) {
                    return false;
                }
                for (var len = 0; len != size;) {
                    // Read message
                    len += await socket.ReceiveAsync(
                        new ArraySegment<byte>(_buffer, len, size - len), SocketFlags.None);
                    ct.ThrowIfCancellationRequested();
                }
                return ValidateResponseBuffer(size);
            }
            catch {
                _logger.Debug($"{socket.RemoteEndPoint} is no opc server.", () => { });
                return false;
            }
            return true;
        }

        /// <summary>
        /// Write hello request buffer
        /// </summary>
        /// <param name="remoteEndpoint"></param>
        /// <returns></returns>
        private int WriteRequestBuffer(EndPoint remoteEndpoint) {
            var size = 0;
            using (var ostrm = new MemoryStream(_buffer, 0, _buffer.Length))
            using (var encoder = new BinaryEncoder(ostrm, _messageContext)) {
                encoder.WriteUInt32(null, TcpMessageType.Hello);
                encoder.WriteUInt32(null, 0);
                encoder.WriteUInt32(null, 0); // ProtocolVersion
                encoder.WriteUInt32(null, TcpMessageLimits.DefaultMaxMessageSize);
                encoder.WriteUInt32(null, TcpMessageLimits.DefaultMaxMessageSize);
                encoder.WriteUInt32(null, TcpMessageLimits.DefaultMaxMessageSize);
                encoder.WriteUInt32(null, TcpMessageLimits.DefaultMaxMessageSize);
                encoder.WriteByteString(null,
                    Encoding.UTF8.GetBytes("opc.tcp://" + remoteEndpoint));
                size = encoder.Close();
            }
            _buffer[4] = (byte)((size & 0x000000FF));
            _buffer[5] = (byte)((size & 0x0000FF00) >> 8);
            _buffer[6] = (byte)((size & 0x00FF0000) >> 16);
            _buffer[7] = (byte)((size & 0xFF000000) >> 24);
            return size;
        }

        /// <summary>
        /// Validate ack response buffer
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        private bool ValidateResponseBuffer(int size) {
            using (var istrm = new MemoryStream(_buffer, 0, size))
            using (var decoder = new BinaryDecoder(istrm, _messageContext)) {
                var protocolVersion = decoder.ReadUInt32(null);
                var sendBufferSize = (int)decoder.ReadUInt32(null);
                var receiveBufferSize = (int)decoder.ReadUInt32(null);
                var maxMessageSize = (int)decoder.ReadUInt32(null);
                var maxChunkCount = (int)decoder.ReadUInt32(null);

                if (sendBufferSize < TcpMessageLimits.MinBufferSize ||
                    receiveBufferSize < TcpMessageLimits.MinBufferSize) {
                    _logger.Debug($"Bad size value read {sendBufferSize} " +
                        $"or {receiveBufferSize}.", () => { });
                    return false;
                }
            }
            return true;
        }

        private enum State {
            Begin,
            Write,
            ReadSize,
            Read,
            Validate
        }

        private State _state;
        private Socket _socket;
        private int _len;
        private int _size;
        private readonly byte[] _buffer;
        private readonly ServiceMessageContext _messageContext;
        private readonly ILogger _logger;
    }
}
