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

import com.facebook.presto.plugin.clp.ClpSplit;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorSplit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Optional;

public class ClpSplitCodec
        implements ConnectorCodec<ConnectorSplit>
{
    @Override
    public byte[] serialize(ConnectorSplit value)
    {
        ClpSplit split = (ClpSplit) value;
        try (ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(byteOut)) {
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
            throw new RuntimeException("Failed to serialize ClpSplit", e);
        }
    }

    @Override
    public ConnectorSplit deserialize(byte[] bytes)
    {
        try (ByteArrayInputStream byteIn = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputStream(byteIn)) {
            String path = in.readUTF();
            ClpSplit.SplitType type = ClpSplit.SplitType.values()[in.readInt()];
            Optional<String> kqlQuery = in.readBoolean()
                    ? Optional.of(in.readUTF())
                    : Optional.empty();
            return new ClpSplit(path, type, kqlQuery);
        }
        catch (IOException e) {
            throw new RuntimeException("Failed to deserialize ClpSplit", e);
        }
    }
}
