package com.yscope.presto.metadata;

import com.facebook.presto.spi.SchemaTableName;
import com.yscope.presto.ClpColumnHandle;

import java.util.Set;

public interface ClpMetadataProvider {
    // TODO(Rui): Think about if it is necessary to return a set of ClpColumnHandle instead of a list of ClpColumnHandle
    public Set<ClpColumnHandle> listTableSchema(SchemaTableName schemaTableName);
    public Set<String> listTables(String schema);
}
