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

import com.facebook.presto.plugin.clp.ClpTableHandle;
import com.facebook.presto.plugin.clp.ClpTableLayoutHandle;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.SchemaTableName;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Optional;

public class ClpTableLayoutHandleCodec
        implements ConnectorCodec<ConnectorTableLayoutHandle>
{
    @Override
    public byte[] serialize(ConnectorTableLayoutHandle value)
    {
        ClpTableLayoutHandle handle = (ClpTableLayoutHandle) value;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(byteOut)) {
            writeClpTableHandle(out, handle.getTable());

            Optional<String> kqlQuery = handle.getKqlQuery();
            out.writeBoolean(kqlQuery.isPresent());
            if (kqlQuery.isPresent()) {
                out.writeUTF(kqlQuery.get());
            }

            Optional<String> metadataSql = handle.getMetadataSql();
            out.writeBoolean(metadataSql.isPresent());
            if (metadataSql.isPresent()) {
                out.writeUTF(metadataSql.get());
            }

            return byteOut.toByteArray();
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to serialize ClpTableLayoutHandle", e);
        }
    }

    @Override
    public ConnectorTableLayoutHandle deserialize(byte[] bytes)
    {
        try (ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputStream(byteIn)) {
            ClpTableHandle table = readClpTableHandle(in);

            Optional<String> kqlQuery = in.readBoolean()
                    ? Optional.of(in.readUTF())
                    : Optional.empty();

            Optional<String> metadataSql = in.readBoolean()
                    ? Optional.of(in.readUTF())
                    : Optional.empty();

            return new ClpTableLayoutHandle(table, kqlQuery, metadataSql);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to deserialize ClpTableLayoutHandle", e);
        }
    }

    private static void writeClpTableHandle(DataOutputStream out, ClpTableHandle handle)
            throws IOException
    {
        out.writeUTF(handle.getSchemaTableName().getSchemaName());
        out.writeUTF(handle.getSchemaTableName().getTableName());
        out.writeUTF(handle.getTablePath());
    }

    private static ClpTableHandle readClpTableHandle(DataInputStream in)
            throws IOException
    {
        String schemaName = in.readUTF();
        String tableName = in.readUTF();
        String tablePath = in.readUTF();
        return new ClpTableHandle(new SchemaTableName(schemaName, tableName), tablePath);
    }
}
