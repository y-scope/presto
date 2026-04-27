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

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorCodec;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorCodecProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ClpCodecProvider
        implements ConnectorCodecProvider
{
    private final ConnectorCodec<ConnectorSplit> splitCodec;
    private final ConnectorCodec<ConnectorTransactionHandle> transactionHandleCodec;
    private final ConnectorCodec<ConnectorTableLayoutHandle> tableLayoutHandleCodec;
    private final ConnectorCodec<ConnectorTableHandle> tableHandleCodec;
    private final ConnectorCodec<ColumnHandle> columnHandleCodec;

    public ClpCodecProvider(TypeManager typeManager)
    {
        requireNonNull(typeManager, "typeManager is null");
        this.splitCodec = new ClpSplitCodec();
        this.transactionHandleCodec = new ClpTransactionHandleCodec();
        this.tableLayoutHandleCodec = new ClpTableLayoutHandleCodec();
        this.tableHandleCodec = new ClpTableHandleCodec();
        this.columnHandleCodec = new ClpColumnHandleCodec(typeManager);
    }

    @Override
    public Optional<ConnectorCodec<ConnectorSplit>> getConnectorSplitCodec()
    {
        return Optional.of(splitCodec);
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTransactionHandle>> getConnectorTransactionHandleCodec()
    {
        return Optional.of(transactionHandleCodec);
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTableLayoutHandle>> getConnectorTableLayoutHandleCodec()
    {
        return Optional.of(tableLayoutHandleCodec);
    }

    @Override
    public Optional<ConnectorCodec<ConnectorTableHandle>> getConnectorTableHandleCodec()
    {
        return Optional.of(tableHandleCodec);
    }

    @Override
    public Optional<ConnectorCodec<ColumnHandle>> getColumnHandleCodec()
    {
        return Optional.of(columnHandleCodec);
    }
}
