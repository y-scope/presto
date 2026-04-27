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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.plugin.clp.ClpColumnHandle;
import com.facebook.presto.plugin.clp.ClpSplit;
import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.plugin.clp.ClpTableLayoutHandle;
import com.facebook.presto.plugin.clp.ClpTransactionHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorCodecProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ClpConnectorCodecProvider
        implements ConnectorCodecProvider
{
    private final TypeManager typeManager;

    public ClpConnectorCodecProvider(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTableHandle>> getConnectorTableHandleCodec()
    {
        return Optional.of(new ClpTableHandleCodec());
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTableLayoutHandle>> getConnectorTableLayoutHandleCodec()
    {
        return Optional.of(new ClpTableLayoutHandleCodec());
    }

    @Override
    public Optional<ConnectorCodec<ColumnHandle>> getColumnHandleCodec()
    {
        return Optional.of(new ClpColumnHandleCodec(typeManager));
    }

    @Override
    public Optional<ConnectorCodec<ConnectorSplit>> getConnectorSplitCodec()
    {
        return Optional.of(new ClpSplitCodec());
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTransactionHandle>> getConnectorTransactionHandleCodec()
    {
        return Optional.of(new ClpTransactionHandleCodec());
    }

    private static class ClpTableHandleCodec
            implements ConnectorCodec<ConnectorTableHandle>
    {
        @Override
        public byte[] serialize(ConnectorTableHandle handle)
        {
            try {
                ClpTableHandle tableHandle = (ClpTableHandle) handle;
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(byteOut);
                out.writeUTF(tableHandle.getSchemaTableName().getSchemaName());
                out.writeUTF(tableHandle.getSchemaTableName().getTableName());
                out.writeUTF(tableHandle.getTablePath());
                return byteOut.toByteArray();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to serialize ClpTableHandle", e);
            }
        }

        @Override
        public ConnectorTableHandle deserialize(byte[] bytes)
        {
            try {
                ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputStream(byteIn);
                String schemaName = in.readUTF();
                String tableName = in.readUTF();
                String tablePath = in.readUTF();
                return new ClpTableHandle(new SchemaTableName(schemaName, tableName), tablePath);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to deserialize ClpTableHandle", e);
            }
        }
    }

    private static class ClpTableLayoutHandleCodec
            implements ConnectorCodec<ConnectorTableLayoutHandle>
    {
        @Override
        public byte[] serialize(ConnectorTableLayoutHandle handle)
        {
            try {
                ClpTableLayoutHandle layoutHandle = (ClpTableLayoutHandle) handle;
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(byteOut);
                // Serialize the table handle
                ClpTableHandle table = layoutHandle.getTable();
                out.writeUTF(table.getSchemaTableName().getSchemaName());
                out.writeUTF(table.getSchemaTableName().getTableName());
                out.writeUTF(table.getTablePath());
                // Serialize kqlQuery
                Optional<String> kqlQuery = layoutHandle.getKqlQuery();
                out.writeBoolean(kqlQuery.isPresent());
                if (kqlQuery.isPresent()) {
                    out.writeUTF(kqlQuery.get());
                }
                // Serialize metadataSql
                Optional<String> metadataSql = layoutHandle.getMetadataSql();
                out.writeBoolean(metadataSql.isPresent());
                if (metadataSql.isPresent()) {
                    out.writeUTF(metadataSql.get());
                }
                return byteOut.toByteArray();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to serialize ClpTableLayoutHandle", e);
            }
        }

        @Override
        public ConnectorTableLayoutHandle deserialize(byte[] bytes)
        {
            try {
                ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputStream(byteIn);
                // Deserialize the table handle
                String schemaName = in.readUTF();
                String tableName = in.readUTF();
                String tablePath = in.readUTF();
                ClpTableHandle table = new ClpTableHandle(new SchemaTableName(schemaName, tableName), tablePath);
                // Deserialize kqlQuery
                Optional<String> kqlQuery = in.readBoolean()
                        ? Optional.of(in.readUTF())
                        : Optional.empty();
                // Deserialize metadataSql
                Optional<String> metadataSql = in.readBoolean()
                        ? Optional.of(in.readUTF())
                        : Optional.empty();
                return new ClpTableLayoutHandle(table, kqlQuery, metadataSql);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to deserialize ClpTableLayoutHandle", e);
            }
        }
    }

    private static class ClpColumnHandleCodec
            implements ConnectorCodec<ColumnHandle>
    {
        private final TypeManager typeManager;

        public ClpColumnHandleCodec(TypeManager typeManager)
        {
            this.typeManager = requireNonNull(typeManager, "typeManager is null");
        }

        @Override
        public byte[] serialize(ColumnHandle handle)
        {
            try {
                ClpColumnHandle columnHandle = (ClpColumnHandle) handle;
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(byteOut);
                out.writeUTF(columnHandle.getColumnName());
                out.writeUTF(columnHandle.getOriginalColumnName());
                out.writeUTF(columnHandle.getColumnType().getTypeSignature().toString());
                return byteOut.toByteArray();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to serialize ClpColumnHandle", e);
            }
        }

        @Override
        public ColumnHandle deserialize(byte[] bytes)
        {
            try {
                ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputStream(byteIn);
                String columnName = in.readUTF();
                String originalColumnName = in.readUTF();
                String typeSignature = in.readUTF();
                Type type = typeManager.getType(TypeSignature.parseTypeSignature(typeSignature));
                return new ClpColumnHandle(columnName, originalColumnName, type);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to deserialize ClpColumnHandle", e);
            }
        }
    }

    private static class ClpSplitCodec
            implements ConnectorCodec<ConnectorSplit>
    {
        @Override
        public byte[] serialize(ConnectorSplit handle)
        {
            try {
                ClpSplit split = (ClpSplit) handle;
                ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(byteOut);
                out.writeUTF(split.getPath());
                out.writeInt(split.getType().ordinal());
                Optional<String> kqlQuery = split.getKqlQuery();
                out.writeBoolean(kqlQuery.isPresent());
                if (kqlQuery.isPresent()) {
                    out.writeUTF(kqlQuery.get());
                }
                return byteOut.toByteArray();
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to serialize ClpSplit", e);
            }
        }

        @Override
        public ConnectorSplit deserialize(byte[] bytes)
        {
            try {
                ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputStream(byteIn);
                String path = in.readUTF();
                ClpSplit.SplitType type = ClpSplit.SplitType.values()[in.readInt()];
                Optional<String> kqlQuery = in.readBoolean()
                        ? Optional.of(in.readUTF())
                        : Optional.empty();
                return new ClpSplit(path, type, kqlQuery);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to deserialize ClpSplit", e);
            }
        }
    }

    private static class ClpTransactionHandleCodec
            implements ConnectorCodec<ConnectorTransactionHandle>
    {
        @Override
        public byte[] serialize(ConnectorTransactionHandle handle)
        {
            // ClpTransactionHandle is a singleton with no data
            return new byte[0];
        }

        @Override
        public ConnectorTransactionHandle deserialize(byte[] bytes)
        {
            // Return the singleton instance
            return ClpTransactionHandle.INSTANCE;
        }
    }
}
