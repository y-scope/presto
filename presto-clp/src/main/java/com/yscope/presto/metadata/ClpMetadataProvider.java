package com.yscope.presto.metadata;

import com.facebook.presto.spi.SchemaTableName;
import com.yscope.presto.ClpColumnHandle;

import java.util.Set;

public interface ClpMetadataProvider {
    public Set<ClpColumnHandle> loadTableSchema(SchemaTableName schemaTableName);

}
