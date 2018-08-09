/* Copyright (c) 1996-2016, OPC Foundation. All rights reserved.
   The source code in this file is covered under a dual-license scenario:
     - RCL: for OPC Foundation members in good-standing
     - GPL V2: everybody else
   RCL license terms accompanied with this source code. See http://opcfoundation.org/License/RCL/1.00/
   GNU General Public License as published by the Free Software Foundation;
   version 2 of the License are accompanied with this source code. See http://opcfoundation.org/License/GPLv2
   This source code is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
*/

namespace Opc.Ua.Encoders {
    using System;
    using System.Collections.Generic;
    using System.Text;
    using System.Xml;
    using System.IO;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Linq;
    using System.Collections;
    using Opc.Ua.Extensions;

    /// <summary>
    /// Reads objects from reader or string
    /// </summary>
    public class JsonDecoderEx : IDecoder, IDisposable {

        /// <inheritdoc/>
        public EncodingType EncodingType => EncodingType.Json;

        /// <inheritdoc/>
        public ServiceMessageContext Context { get; }

        /// <summary>
        /// Create decoder
        /// </summary>
        /// <param name="context"></param>
        /// <param name="json"></param>
        public JsonDecoderEx(ServiceMessageContext context, string json) :
            this(context, new JsonTextReader(new StringReader(json))) {
        }

        /// <summary>
        /// Create decoder
        /// </summary>
        /// <param name="context"></param>
        /// <param name="stream"></param>
        public JsonDecoderEx(ServiceMessageContext context, Stream stream) :
            this(context, new JsonTextReader(new StreamReader(stream))) {
        }

        /// <summary>
        /// Create decoder
        /// </summary>
        /// <param name="context"></param>
        /// <param name="reader"></param>
        public JsonDecoderEx(ServiceMessageContext context, JsonTextReader reader) {
            Context = context;
            var root = JToken.ReadFrom(reader, new JsonLoadSettings {
                CommentHandling = CommentHandling.Ignore,
                LineInfoHandling = LineInfoHandling.Ignore
            });
            _stack.Push(root as JObject ?? throw new ArgumentException(nameof(reader)));
            reader.Close();
        }

        /// <inheritdoc/>
        public void Dispose() {
            // No op
        }

        /// <inheritdoc/>
        public void PushNamespace(string namespaceUri) {
            // No op
        }

        /// <inheritdoc/>
        public void PopNamespace() {
            // No op
        }

        /// <inheritdoc/>
        public void SetMappingTables(NamespaceTable namespaceUris, StringTable serverUris) {
            _namespaceMappings = null;

            if (namespaceUris != null && Context.NamespaceUris != null) {
                _namespaceMappings = Context.NamespaceUris.CreateMapping(namespaceUris, false);
            }

            _serverMappings = null;

            if (serverUris != null && Context.ServerUris != null) {
                _serverMappings = Context.ServerUris.CreateMapping(serverUris, false);
            }
        }






        /// <inheritdoc/>
        public bool ReadBoolean(string fieldName) =>
            _stack.Peek().TryGetValue(fieldName, out var value) && (bool)value;

        /// <inheritdoc/>
        public sbyte ReadSByte(string fieldName) => ReadInteger(fieldName,
            v => (sbyte) (v < sbyte.MinValue || v > sbyte.MaxValue ? 0 : v));

        /// <inheritdoc/>
        public byte ReadByte(string fieldName) => ReadInteger(fieldName,
            v => (byte) (v < byte.MinValue || v > byte.MaxValue ? 0 : v));

        /// <inheritdoc/>
        public short ReadInt16(string fieldName) => ReadInteger(fieldName,
            v => (short) (v < short.MinValue || v > short.MaxValue ? 0 : v));

        /// <inheritdoc/>
        public ushort ReadUInt16(string fieldName) => ReadInteger(fieldName,
            v => (ushort) (v < ushort.MinValue || v > ushort.MaxValue ? 0 : v));

        /// <inheritdoc/>
        public int ReadInt32(string fieldName) => ReadInteger(fieldName,
            v => (int) (v < int.MinValue || v > int.MaxValue ? 0 : v));

        /// <inheritdoc/>
        public uint ReadUInt32(string fieldName) => ReadInteger(fieldName,
            v => (uint) (v < uint.MinValue || v > uint.MaxValue ? 0 : v));

        /// <inheritdoc/>
        public long ReadInt64(string fieldName) => ReadInteger(fieldName,
            v => v);

        /// <inheritdoc/>
        public ulong ReadUInt64(string fieldName) => ReadInteger(fieldName,
            v => (ulong)v);

        /// <inheritdoc/>
        public float ReadFloat(string fieldName) => ReadDouble(fieldName,
            v => (float) (v < float.MinValue || v > float.MaxValue ? 0 : v));

        /// <inheritdoc/>
        public double ReadDouble(string fieldName) => ReadDouble(fieldName,
            v => v);

        /// <inheritdoc/>
        public string ReadString(string fieldName) {
            if (!_stack.Peek().TryGetValue(fieldName, out var token)) {
                return null;
            }
            return token.ToString();
        }

        /// <inheritdoc/>
        public DateTime ReadDateTime(string fieldName) => ReadNullable(fieldName,
            t => XmlConvert.ToDateTime(t.ToString(), XmlDateTimeSerializationMode.Utc));

        /// <inheritdoc/>
        public Uuid ReadGuid(string fieldName) =>
            new Uuid(ReadString(fieldName));

        /// <inheritdoc/>
        public byte[] ReadByteString(string fieldName) => Read(fieldName,
            t => Convert.FromBase64String(t.ToString()));

        /// <inheritdoc/>
        public XmlElement ReadXmlElement(string fieldName) {
            return Read(fieldName, t => {
                var bytes = t.ToObject<byte[]>();
                if (bytes != null && bytes.Length > 0) {
                    var document = new XmlDocument {
                        InnerXml = Encoding.UTF8.GetString(bytes)
                    };
                    return document.DocumentElement;
                }
                return null;
            });
        }

        /// <inheritdoc/>
        public NodeId ReadNodeId(string fieldName) {
            if (!_stack.Peek().TryGetValue(fieldName, out var token)) {
                return null;
            }
            if (token is JObject o) {
                _stack.Push(o);
                var id = ReadString("Id");
                var uri = ReadString("Uri");
                if (string.IsNullOrEmpty(uri)) {
                    var index = (ushort)ReadUInt32("Index");
                    uri = Context.NamespaceUris.GetString(index);
                }
                _stack.Pop();
                return NodeId.Parse(id);
            }
            try {
                return NodeId.Parse(token.ToString());
            }
            catch {
                return token.ToString().ToNodeId(Context);
            }
        }

        /// <inheritdoc/>
        public ExpandedNodeId ReadExpandedNodeId(string fieldName) {
            if (!_stack.Peek().TryGetValue(fieldName, out var token)) {
                return null;
            }
            if (token is JObject o) {
                _stack.Push(o);
                var id = ReadString("Id");
                var uri = ReadString("Uri");
                if (string.IsNullOrEmpty(uri)) {
                    var index = (ushort)ReadUInt32("Index");
                    uri = Context.NamespaceUris.GetString(index);
                }
                var serverIndex = (ushort)ReadUInt32("ServerIndex");
                if (serverIndex == 0) {
                    var server = ReadString("ServerUri");
                    serverIndex = Context.NamespaceUris.GetIndexOrAppend(server);
                }
                _stack.Pop();
                return new ExpandedNodeId(NodeId.Parse(id), uri, serverIndex);
            }
            try {
                return ExpandedNodeId.Parse(token.ToString());
            }
            catch {
                return token.ToString().ToExpandedNodeId(Context);
            }
        }

        /// <inheritdoc/>
        public StatusCode ReadStatusCode(string fieldName) {
            if (!_stack.Peek().TryGetValue(fieldName, out var token)) {
                return 0;
            }
            if (token is JObject o) {
                _stack.Push(o);
                var code = new StatusCode(ReadUInt32("Code"));
                _stack.Pop();
                return code;
            }
            return ReadInteger(fieldName, v =>
                (uint)(v < uint.MinValue || v > uint.MaxValue ? 0 : v));
        }

        /// <summary>
        /// Reads an DiagnosticInfo from the stream.
        /// </summary>
        public DiagnosticInfo ReadDiagnosticInfo(string fieldName) {
            if (!_stack.Peek().TryGetValue(fieldName, out var token)) {
                return null;
            }
            if (token is JObject o) {
                _stack.Push(o);
                var di = new DiagnosticInfo {
                    SymbolicId = ReadInt32("SymbolicId"),
                    NamespaceUri = ReadInt32("NamespaceUri"),
                    Locale = ReadInt32("Locale"),
                    LocalizedText = ReadInt32("LocalizedText"),
                    AdditionalInfo = ReadString("AdditionalInfo"),
                    InnerStatusCode = ReadStatusCode("InnerStatusCode"),
                    InnerDiagnosticInfo =
                        ReadDiagnosticInfo("InnerDiagnosticInfo")
                };
                _stack.Pop();
                return di;
            }
            return null;
        }

        /// <summary>
        /// Reads an QualifiedName from the stream.
        /// </summary>
        public QualifiedName ReadQualifiedName(string fieldName) {
            if (!_stack.Peek().TryGetValue(fieldName, out var token)) {
                return null;
            }
            if (token is JObject o) {
                _stack.Push(o);
                var name = ReadString("Name");
                var index = ReadUInt16("Uri");
                if (index == 0) {
                    // Non reversible
                    index = (ushort)ReadUInt32("Index");
                    if (index == 0) {
                        var uri = ReadString("Uri");
                        index = Context.NamespaceUris.GetIndexOrAppend(uri);
                    }
                }
                _stack.Pop();
                return new QualifiedName(name, index);
            }
            return null;
        }

        /// <summary>
        /// Reads an LocalizedText from the stream.
        /// </summary>
        public LocalizedText ReadLocalizedText(string fieldName) {

            if (!ReadField(fieldName, out var token)) {
                return null;
            }

            string locale = null;
            string text = null;

            if (!(token is Dictionary<string, object> value)) {
                text = token as string;

                if (text != null) {
                    return new LocalizedText(text);
                }

                return null;
            }

            try {
                _stack.Push(value);

                if (value.ContainsKey("Locale")) {
                    locale = ReadString("Locale");
                }

                if (value.ContainsKey("Text")) {
                    text = ReadString("Text");
                }
            }
            finally {
                _stack.Pop();
            }

            return new LocalizedText(locale, text);
        }

        private Variant ReadVariantBody(string fieldName, BuiltInType type) {
            switch (type) {
                case BuiltInType.Boolean: { return new Variant(ReadBoolean(fieldName), TypeInfo.Scalars.Boolean); }
                case BuiltInType.SByte: { return new Variant(ReadSByte(fieldName), TypeInfo.Scalars.SByte); }
                case BuiltInType.Byte: { return new Variant(ReadByte(fieldName), TypeInfo.Scalars.Byte); }
                case BuiltInType.Int16: { return new Variant(ReadInt16(fieldName), TypeInfo.Scalars.Int16); }
                case BuiltInType.UInt16: { return new Variant(ReadUInt16(fieldName), TypeInfo.Scalars.UInt16); }
                case BuiltInType.Int32: { return new Variant(ReadInt32(fieldName), TypeInfo.Scalars.Int32); }
                case BuiltInType.UInt32: { return new Variant(ReadUInt32(fieldName), TypeInfo.Scalars.UInt32); }
                case BuiltInType.Int64: { return new Variant(ReadInt64(fieldName), TypeInfo.Scalars.Int64); }
                case BuiltInType.UInt64: { return new Variant(ReadUInt64(fieldName), TypeInfo.Scalars.UInt64); }
                case BuiltInType.Float: { return new Variant(ReadFloat(fieldName), TypeInfo.Scalars.Float); }
                case BuiltInType.Double: { return new Variant(ReadDouble(fieldName), TypeInfo.Scalars.Double); }
                case BuiltInType.String: { return new Variant(ReadString(fieldName), TypeInfo.Scalars.String); }
                case BuiltInType.ByteString: { return new Variant(ReadByteString(fieldName), TypeInfo.Scalars.ByteString); }
                case BuiltInType.DateTime: { return new Variant(ReadDateTime(fieldName), TypeInfo.Scalars.DateTime); }
                case BuiltInType.Guid: { return new Variant(ReadGuid(fieldName), TypeInfo.Scalars.Guid); }
                case BuiltInType.NodeId: { return new Variant(ReadNodeId(fieldName), TypeInfo.Scalars.NodeId); }
                case BuiltInType.ExpandedNodeId: { return new Variant(ReadExpandedNodeId(fieldName), TypeInfo.Scalars.ExpandedNodeId); }
                case BuiltInType.QualifiedName: { return new Variant(ReadQualifiedName(fieldName), TypeInfo.Scalars.QualifiedName); }
                case BuiltInType.LocalizedText: { return new Variant(ReadLocalizedText(fieldName), TypeInfo.Scalars.LocalizedText); }
                case BuiltInType.StatusCode: { return new Variant(ReadStatusCode(fieldName), TypeInfo.Scalars.StatusCode); }
                case BuiltInType.XmlElement: { return new Variant(ReadXmlElement(fieldName), TypeInfo.Scalars.XmlElement); }
                case BuiltInType.ExtensionObject: { return new Variant(ReadExtensionObject(fieldName), TypeInfo.Scalars.ExtensionObject); }
                case BuiltInType.Variant: { return new Variant(ReadVariant(fieldName), TypeInfo.Scalars.Variant); }
            }

            return Variant.Null;
        }

        private Variant ReadVariantArrayBody(string fieldName, BuiltInType type) {
            switch (type) {
                case BuiltInType.Boolean: { return new Variant(ReadBooleanArray(fieldName), TypeInfo.Arrays.Boolean); }
                case BuiltInType.SByte: { return new Variant(ReadSByteArray(fieldName), TypeInfo.Arrays.SByte); }
                case BuiltInType.Byte: { return new Variant(ReadByteArray(fieldName), TypeInfo.Arrays.Byte); }
                case BuiltInType.Int16: { return new Variant(ReadInt16Array(fieldName), TypeInfo.Arrays.Int16); }
                case BuiltInType.UInt16: { return new Variant(ReadUInt16Array(fieldName), TypeInfo.Arrays.UInt16); }
                case BuiltInType.Int32: { return new Variant(ReadInt32Array(fieldName), TypeInfo.Arrays.Int32); }
                case BuiltInType.UInt32: { return new Variant(ReadUInt32Array(fieldName), TypeInfo.Arrays.UInt32); }
                case BuiltInType.Int64: { return new Variant(ReadInt64Array(fieldName), TypeInfo.Arrays.Int64); }
                case BuiltInType.UInt64: { return new Variant(ReadUInt64Array(fieldName), TypeInfo.Arrays.UInt64); }
                case BuiltInType.Float: { return new Variant(ReadFloatArray(fieldName), TypeInfo.Arrays.Float); }
                case BuiltInType.Double: { return new Variant(ReadDoubleArray(fieldName), TypeInfo.Arrays.Double); }
                case BuiltInType.String: { return new Variant(ReadStringArray(fieldName), TypeInfo.Arrays.String); }
                case BuiltInType.ByteString: { return new Variant(ReadByteStringArray(fieldName), TypeInfo.Arrays.ByteString); }
                case BuiltInType.DateTime: { return new Variant(ReadDateTimeArray(fieldName), TypeInfo.Arrays.DateTime); }
                case BuiltInType.Guid: { return new Variant(ReadGuidArray(fieldName), TypeInfo.Arrays.Guid); }
                case BuiltInType.NodeId: { return new Variant(ReadNodeIdArray(fieldName), TypeInfo.Arrays.NodeId); }
                case BuiltInType.ExpandedNodeId: { return new Variant(ReadExpandedNodeIdArray(fieldName), TypeInfo.Arrays.ExpandedNodeId); }
                case BuiltInType.QualifiedName: { return new Variant(ReadQualifiedNameArray(fieldName), TypeInfo.Arrays.QualifiedName); }
                case BuiltInType.LocalizedText: { return new Variant(ReadLocalizedTextArray(fieldName), TypeInfo.Arrays.LocalizedText); }
                case BuiltInType.StatusCode: { return new Variant(ReadStatusCodeArray(fieldName), TypeInfo.Arrays.StatusCode); }
                case BuiltInType.XmlElement: { return new Variant(ReadXmlElementArray(fieldName), TypeInfo.Arrays.XmlElement); }
                case BuiltInType.ExtensionObject: { return new Variant(ReadExtensionObjectArray(fieldName), TypeInfo.Arrays.ExtensionObject); }
                case BuiltInType.Variant: { return new Variant(ReadVariantArray(fieldName), TypeInfo.Arrays.Variant); }
            }

            return Variant.Null;
        }

        /// <summary>
        /// Reads an Variant from the stream.
        /// </summary>
        public Variant ReadVariant(string fieldName) {

            if (!ReadField(fieldName, out var token)) {
                return Variant.Null;
            }


            if (!(token is Dictionary<string, object> value)) {
                return Variant.Null;
            }

            // check the nesting level for avoiding a stack overflow.
            if (_nestingLevel > Context.MaxEncodingNestingLevels) {
                throw ServiceResultException.Create(
                    StatusCodes.BadEncodingLimitsExceeded,
                    "Maximum nesting level of {0} was exceeded",
                    Context.MaxEncodingNestingLevels);
            }
            try {
                _nestingLevel++;
                _stack.Push(value);

                var type = (BuiltInType)ReadByte("Type");

                var context = _stack.Peek() as Dictionary<string, object>;
                if (!context.TryGetValue("Body", out token)) {
                    return Variant.Null;
                }

                if (token is ICollection) {
                    var array = ReadVariantArrayBody("Body", type);
                    var dimensions = ReadInt32Array("Dimensions");

                    if (array.Value is ICollection && dimensions != null && dimensions.Count > 1) {
                        array = new Variant(new Matrix((Array)array.Value, type, dimensions.ToArray()));
                    }

                    return array;
                }
                else {
                    return ReadVariantBody("Body", type);
                }
            }
            finally {
                _nestingLevel--;
                _stack.Pop();
            }
        }

        /// <summary>
        /// Reads an DataValue from the stream.
        /// </summary>
        public DataValue ReadDataValue(string fieldName) {

            if (!ReadField(fieldName, out var token)) {
                return null;
            }


            if (!(token is Dictionary<string, object> value)) {
                return null;
            }

            var dv = new DataValue();

            try {
                _stack.Push(value);

                dv.WrappedValue = ReadVariant("Value");
                dv.StatusCode = ReadStatusCode("StatusCode");
                dv.SourceTimestamp = ReadDateTime("SourceTimestamp");
                dv.SourcePicoseconds = ReadUInt16("SourcePicoseconds");
                dv.ServerTimestamp = ReadDateTime("ServerTimestamp");
                dv.ServerPicoseconds = ReadUInt16("ServerPicoseconds");
            }
            finally {
                _stack.Pop();
            }

            return dv;
        }

        private void EncodeAsJson(JsonTextWriter writer, object value) {

            if (value is Dictionary<string, object> map) {
                EncodeAsJson(writer, map);
                return;
            }


            if (value is List<object> list) {
                writer.WriteStartArray();

                foreach (var element in list) {
                    EncodeAsJson(writer, element);
                }

                writer.WriteStartArray();
                return;
            }

            writer.WriteValue(value);
        }

        private void EncodeAsJson(JsonTextWriter writer, Dictionary<string, object> value) {
            writer.WriteStartObject();

            foreach (var field in value) {
                writer.WritePropertyName(field.Key);
                EncodeAsJson(writer, field.Value);
            }

            writer.WriteEndObject();
        }

        /// <summary>
        /// Reads an extension object from the stream.
        /// </summary>
        public ExtensionObject ReadExtensionObject(string fieldName) {

            if (!ReadField(fieldName, out var token)) {
                return null;
            }


            if (!(token is Dictionary<string, object> value)) {
                return null;
            }

            try {
                _stack.Push(value);

                var typeId = ReadNodeId("TypeId");

                var absoluteId = NodeId.ToExpandedNodeId(typeId, Context.NamespaceUris);

                if (!NodeId.IsNull(typeId) && NodeId.IsNull(absoluteId)) {
                    Utils.Trace("Cannot de-serialized extension objects if the NamespaceUri is not in the NamespaceTable: Type = {0}", typeId);
                }

                var encoding = ReadByte("Encoding");

                if (encoding == 1) {
                    var bytes = ReadByteString("Body");
                    return new ExtensionObject(typeId, bytes);
                }

                if (encoding == 2) {
                    var xml = ReadXmlElement("Body");
                    return new ExtensionObject(typeId, xml);
                }

                var systemType = Context.Factory.GetSystemType(typeId);

                if (systemType != null) {
                    var encodeable = ReadEncodeable("Body", systemType);
                    return new ExtensionObject(typeId, encodeable);
                }

                var ostrm = new MemoryStream();

                using (var writer = new JsonTextWriter(new StreamWriter(ostrm))) {
                    EncodeAsJson(writer, token);
                }

                return new ExtensionObject(typeId, ostrm.ToArray());
            }
            finally {
                _stack.Pop();
            }
        }

        /// <summary>
        /// Reads an encodeable object from the stream.
        /// </summary>
        public IEncodeable ReadEncodeable(
            string fieldName,
            Type systemType) {
            if (systemType == null) {
                throw new ArgumentNullException(nameof(systemType));
            }


            if (!ReadField(fieldName, out var token)) {
                return null;
            }


            if (!(Activator.CreateInstance(systemType) is IEncodeable value)) {
                throw new ServiceResultException(StatusCodes.BadDecodingError, Utils.Format("Type does not support IEncodeable interface: '{0}'", systemType.FullName));
            }

            // check the nesting level for avoiding a stack overflow.
            if (_nestingLevel > Context.MaxEncodingNestingLevels) {
                throw ServiceResultException.Create(
                    StatusCodes.BadEncodingLimitsExceeded,
                    "Maximum nesting level of {0} was exceeded",
                    Context.MaxEncodingNestingLevels);
            }

            _nestingLevel++;

            try {
                _stack.Push(token);

                value.Decode(this);
            }
            finally {
                _stack.Pop();
            }

            _nestingLevel--;

            return value;
        }

        /// <summary>
        ///  Reads an enumerated value from the stream.
        /// </summary>
        public Enum ReadEnumerated(string fieldName, Type enumType) {
            if (enumType == null) {
                throw new ArgumentNullException(nameof(enumType));
            }

            return (Enum)Enum.ToObject(enumType, ReadInt32(fieldName));
        }

        /// <summary>
        /// Reads a boolean array from the stream.
        /// </summary>
        public BooleanCollection ReadBooleanArray(string fieldName) {
            var values = new BooleanCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    values.Add(ReadBoolean(null));
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads a sbyte array from the stream.
        /// </summary>
        public SByteCollection ReadSByteArray(string fieldName) {
            var values = new SByteCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    values.Add(ReadSByte(null));
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads a byte array from the stream.
        /// </summary>
        public ByteCollection ReadByteArray(string fieldName) {
            var values = new ByteCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    values.Add(ReadByte(null));
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads a short array from the stream.
        /// </summary>
        public Int16Collection ReadInt16Array(string fieldName) {
            var values = new Int16Collection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    values.Add(ReadInt16(null));
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads a ushort array from the stream.
        /// </summary>
        public UInt16Collection ReadUInt16Array(string fieldName) {
            var values = new UInt16Collection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    values.Add(ReadUInt16(null));
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads a int array from the stream.
        /// </summary>
        public Int32Collection ReadInt32Array(string fieldName) {
            var values = new Int32Collection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    values.Add(ReadInt32(null));
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads a uint array from the stream.
        /// </summary>
        public UInt32Collection ReadUInt32Array(string fieldName) {
            var values = new UInt32Collection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    values.Add(ReadUInt32(null));
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads a long array from the stream.
        /// </summary>
        public Int64Collection ReadInt64Array(string fieldName) {
            var values = new Int64Collection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    values.Add(ReadInt64(null));
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads a ulong array from the stream.
        /// </summary>
        public UInt64Collection ReadUInt64Array(string fieldName) {
            var values = new UInt64Collection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    values.Add(ReadUInt64(null));
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads a float array from the stream.
        /// </summary>
        public FloatCollection ReadFloatArray(string fieldName) {
            var values = new FloatCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    values.Add(ReadFloat(null));
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads a double array from the stream.
        /// </summary>
        public DoubleCollection ReadDoubleArray(string fieldName) {
            var values = new DoubleCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    values.Add(ReadDouble(null));
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads a string array from the stream.
        /// </summary>
        public StringCollection ReadStringArray(string fieldName) {
            var values = new StringCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    values.Add(ReadString(null));
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads a UTC date/time array from the stream.
        /// </summary>
        public DateTimeCollection ReadDateTimeArray(string fieldName) {
            var values = new DateTimeCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    values.Add(ReadDateTime(null));
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads a GUID array from the stream.
        /// </summary>
        public UuidCollection ReadGuidArray(string fieldName) {
            var values = new UuidCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    var element = ReadGuid(null);
                    values.Add(element);
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads a byte string array from the stream.
        /// </summary>
        public ByteStringCollection ReadByteStringArray(string fieldName) {
            var values = new ByteStringCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    var element = ReadByteString(null);
                    values.Add(element);
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads an XmlElement array from the stream.
        /// </summary>
        public XmlElementCollection ReadXmlElementArray(string fieldName) {
            var values = new XmlElementCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    var element = ReadXmlElement(null);
                    values.Add(element);
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads an NodeId array from the stream.
        /// </summary>
        public NodeIdCollection ReadNodeIdArray(string fieldName) {
            var values = new NodeIdCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    var element = ReadNodeId(null);
                    values.Add(element);
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads an ExpandedNodeId array from the stream.
        /// </summary>
        public ExpandedNodeIdCollection ReadExpandedNodeIdArray(string fieldName) {
            var values = new ExpandedNodeIdCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    var element = ReadExpandedNodeId(null);
                    values.Add(element);
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads an StatusCode array from the stream.
        /// </summary>
        public StatusCodeCollection ReadStatusCodeArray(string fieldName) {
            var values = new StatusCodeCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    var element = ReadStatusCode(null);
                    values.Add(element);
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads an DiagnosticInfo array from the stream.
        /// </summary>
        public DiagnosticInfoCollection ReadDiagnosticInfoArray(string fieldName) {
            var values = new DiagnosticInfoCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    var element = ReadDiagnosticInfo(null);
                    values.Add(element);
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads an QualifiedName array from the stream.
        /// </summary>
        public QualifiedNameCollection ReadQualifiedNameArray(string fieldName) {
            var values = new QualifiedNameCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    var element = ReadQualifiedName(null);
                    values.Add(element);
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads an LocalizedText array from the stream.
        /// </summary>
        public LocalizedTextCollection ReadLocalizedTextArray(string fieldName) {
            var values = new LocalizedTextCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    var element = ReadLocalizedText(null);
                    values.Add(element);
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads an Variant array from the stream.
        /// </summary>
        public VariantCollection ReadVariantArray(string fieldName) {
            var values = new VariantCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    var element = ReadVariant(null);
                    values.Add(element);
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads an DataValue array from the stream.
        /// </summary>
        public DataValueCollection ReadDataValueArray(string fieldName) {
            var values = new DataValueCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    var element = ReadDataValue(null);
                    values.Add(element);
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads an array of extension objects from the stream.
        /// </summary>
        public ExtensionObjectCollection ReadExtensionObjectArray(string fieldName) {
            var values = new ExtensionObjectCollection();


            if (!ReadArrayField(fieldName, out var token)) {
                return values;
            }

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    var element = ReadExtensionObject(null);
                    values.Add(element);
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads an encodeable object array from the stream.
        /// </summary>
        public Array ReadEncodeableArray(string fieldName, Type systemType) {
            if (systemType == null) {
                throw new ArgumentNullException(nameof(systemType));
            }


            if (!ReadArrayField(fieldName, out var token)) {
                return Array.CreateInstance(systemType, 0);
            }

            var values = Array.CreateInstance(systemType, token.Count);

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    var element = ReadEncodeable(null, systemType);
                    values.SetValue(element, ii);
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }

        /// <summary>
        /// Reads an enumerated value array from the stream.
        /// </summary>
        public Array ReadEnumeratedArray(string fieldName, Type enumType) {
            if (enumType == null) {
                throw new ArgumentNullException(nameof(enumType));
            }


            if (!ReadArrayField(fieldName, out var token)) {
                return Array.CreateInstance(enumType, 0);
            }

            var values = Array.CreateInstance(enumType, token.Count);

            for (var ii = 0; ii < token.Count; ii++) {
                try {
                    _stack.Push(token[ii]);
                    var element = ReadEnumerated(null, enumType);
                    values.SetValue(element, ii);
                }
                finally {
                    _stack.Pop();
                }
            }

            return values;
        }




        /// <summary>
        /// Reads integer
        /// </summary>
        private T ReadInteger<T>(string fieldName, Func<long, T> convert) {
            if (!_stack.Peek().TryGetValue(fieldName, out var token)) {
                return default(T);
            }
            return convert(token.ToObject<long>());
        }

        /// <summary>
        /// Reads double
        /// </summary>
        private T ReadDouble<T>(string fieldName, Func<double, T> convert) {
            if (!_stack.Peek().TryGetValue(fieldName, out var token)) {
                return default(T);
            }
            return convert(token.ToObject<double>());
        }

        private T ReadNullable<T>(string fieldName, Func<JToken, T> fallback)
            where T : struct {
            if (!_stack.Peek().TryGetValue(fieldName, out var token)) {
                return default(T);
            }
            var value = token.ToObject<T?>();
            if (value != null) {
                return value.Value;
            }
            try {
                return fallback(token);
            }
            catch {
                return default(T);
            }
        }

        private T Read<T>(string fieldName, Func<JToken, T> fallback)
            where T : class {
            if (!_stack.Peek().TryGetValue(fieldName, out var token)) {
                return default(T);
            }
            var value = token.ToObject<T>();
            if (value != null) {
                return value;
            }
            try {
                return fallback(token);
            }
            catch {
                return default(T);
            }
        }



        private bool ReadArrayField(string fieldName, out List<object> array) {
            object token = array = null;

            if (!ReadField(fieldName, out token)) {
                return false;
            }

            array = token as List<object>;

            if (array == null) {
                return false;
            }

            if (Context.MaxArrayLength > 0 && Context.MaxArrayLength < array.Count) {
                throw new ServiceResultException(StatusCodes.BadEncodingLimitsExceeded);
            }

            return true;
        }

        public bool ReadField(string fieldName, out object token) {
            token = _stack.Peek();
            if (string.IsNullOrEmpty(fieldName)) {
                return true;
            }
            if (!(token is Dictionary<string, object> context) ||
                !context.TryGetValue(fieldName, out token)) {
                return false;
            }
            return true;
        }

        private ushort[] _namespaceMappings;
        private ushort[] _serverMappings;
        private readonly Stack<JObject> _stack = new Stack<JObject>();
    }
}
