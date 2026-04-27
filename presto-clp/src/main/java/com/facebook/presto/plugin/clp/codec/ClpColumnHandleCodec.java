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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorCodec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;

import static java.util.Objects.requireNonNull;

public class ClpColumnHandleCodec
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
