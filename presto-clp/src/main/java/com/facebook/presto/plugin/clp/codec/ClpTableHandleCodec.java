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
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.SchemaTableName;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

public class ClpTableHandleCodec
        implements ConnectorCodec<ConnectorTableHandle>
{
    // Wire format (C++ ClpConnectorProtocol::deserialize for ConnectorTableHandle):
    //   schemaName : writeUTF
    //   tableName  : writeUTF
    //   tablePath  : writeUTF

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
