/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.plugin.clp.codec;

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.plugin.clp.ClpColumnHandle;
import com.facebook.presto.plugin.clp.ClpSplit;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.plugin.clp.ClpTableLayoutHandle;
import com.facebook.presto.plugin.clp.ClpTransactionHandle;
import com.facebook.presto.spi.SchemaTableName;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

/**
 * Byte fixture tests that pin the exact wire format for cross-language contract verification.
 * These byte sequences must match the C++ ClpConnectorProtocol::deserialize() implementations exactly.
 *
 * Wire format: Java DataOutputStream big-endian
 * - String: 2-byte BE length + UTF-8 bytes (writeUTF)
 * - int: 4 bytes big-endian
 * - boolean: 1 byte (0/1)
 * - Optional: boolean flag + conditional data
 */
public class TestClpCodecByteFixtures
{
    private static final TypeManager TYPE_MANAGER = new TypeManager()
    {
        @Override
        public Type getType(TypeSignature signature)
        {
            switch (signature.getBase()) {
                case "varchar":
                    return VarcharType.createUnboundedVarcharType();
                case "bigint":
                    return BigintType.BIGINT;
                default:
                    throw new IllegalArgumentException("Unknown type: " + signature);
            }
        }

        @Override
        public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
        {
            return getType(new TypeSignature(baseTypeName));
        }

        @Override
        public boolean canCoerce(Type actualType, Type expectedType)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<Type> getTypes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasType(TypeSignature signature)
        {
            return getType(signature) != null;
        }
    };

    @Test
    public void testColumnHandleBytes()
            throws IOException
    {
        ClpColumnHandleCodec codec = new ClpColumnHandleCodec(TYPE_MANAGER);
        ClpColumnHandle handle = new ClpColumnHandle("ts", "ts", BigintType.BIGINT);
        byte[] bytes = codec.serialize(handle);

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(expected);
        out.writeUTF("ts");
        out.writeUTF("ts");
        out.writeUTF("bigint");
        out.flush();

        assertEquals(bytes, expected.toByteArray());
    }

    @Test
    public void testSplitArchiveWithKqlBytes()
            throws IOException
    {
        ClpSplitCodec codec = new ClpSplitCodec();
        ClpSplit split = new ClpSplit("/a", ClpSplit.SplitType.ARCHIVE, Optional.of("*"));
        byte[] bytes = codec.serialize(split);

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(expected);
        out.writeUTF("/a");       // path
        out.writeInt(0);          // type ordinal: ARCHIVE = 0
        out.writeBoolean(true);   // kqlQuery present
        out.writeUTF("*");        // kqlQuery
        out.flush();

        assertEquals(bytes, expected.toByteArray());
    }

    @Test
    public void testSplitIrNoKqlBytes()
            throws IOException
    {
        ClpSplitCodec codec = new ClpSplitCodec();
        ClpSplit split = new ClpSplit("/b", ClpSplit.SplitType.IR, Optional.empty());
        byte[] bytes = codec.serialize(split);

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(expected);
        out.writeUTF("/b");       // path
        out.writeInt(1);          // type ordinal: IR = 1
        out.writeBoolean(false);  // kqlQuery absent
        out.flush();

        assertEquals(bytes, expected.toByteArray());
    }

    @Test
    public void testTransactionHandleBytes()
    {
        ClpTransactionHandleCodec codec = new ClpTransactionHandleCodec();
        byte[] bytes = codec.serialize(ClpTransactionHandle.INSTANCE);
        assertEquals(bytes.length, 0);
        assertEquals(bytes, new byte[0]);
    }

    @Test
    public void testTableHandleBytes()
            throws IOException
    {
        ClpTableHandleCodec codec = new ClpTableHandleCodec();
        ClpTableHandle handle = new ClpTableHandle(new SchemaTableName("default", "logs"), "/data");
        byte[] bytes = codec.serialize(handle);

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(expected);
        out.writeUTF("default");
        out.writeUTF("logs");
        out.writeUTF("/data");
        out.flush();

        assertEquals(bytes, expected.toByteArray());
    }

    @Test
    public void testTableLayoutHandleMinimalBytes()
            throws IOException
    {
        ClpTableLayoutHandleCodec codec = new ClpTableLayoutHandleCodec();
        ClpTableHandle table = new ClpTableHandle(new SchemaTableName("clp", "logs"), "");
        ClpTableLayoutHandle handle = new ClpTableLayoutHandle(table, Optional.empty(), Optional.empty());

        byte[] bytes = codec.serialize(handle);

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(expected);
        out.writeUTF("clp");       // schemaName
        out.writeUTF("logs");      // tableName
        out.writeUTF("");          // tablePath
        out.writeBoolean(false);   // kqlQuery absent
        out.writeBoolean(false);   // metadataSql absent
        out.flush();

        assertEquals(bytes, expected.toByteArray());
    }

    @Test
    public void testTableLayoutHandleWithBothOptionals()
            throws IOException
    {
        ClpTableLayoutHandleCodec codec = new ClpTableLayoutHandleCodec();
        ClpTableHandle table = new ClpTableHandle(new SchemaTableName("clp", "logs"), "/data");
        ClpTableLayoutHandle handle = new ClpTableLayoutHandle(table, Optional.of("ERROR"), Optional.of("(end_timestamp > 100)"));

        byte[] bytes = codec.serialize(handle);

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(expected);
        out.writeUTF("clp");
        out.writeUTF("logs");
        out.writeUTF("/data");
        out.writeBoolean(true);
        out.writeUTF("ERROR");
        out.writeBoolean(true);
        out.writeUTF("(end_timestamp > 100)");
        out.flush();

        assertEquals(bytes, expected.toByteArray());
    }

    @Test
    public void testEmptyTablePathBytes()
            throws IOException
    {
        ClpTableHandleCodec codec = new ClpTableHandleCodec();
        ClpTableHandle handle = new ClpTableHandle(new SchemaTableName("s", "t"), "");
        byte[] bytes = codec.serialize(handle);

        ByteArrayOutputStream expected = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(expected);
        out.writeUTF("s");
        out.writeUTF("t");
        out.writeUTF("");
        out.flush();

        assertEquals(bytes, expected.toByteArray());
    }
}
