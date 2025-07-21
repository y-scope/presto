package com.facebook.presto.plugin.clp.metadata.filter;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ClpMetadataFilter
{
    @JsonProperty("columnName")
    public String columnName;

    @JsonProperty("metadataDatabaseSpecific")
    public MetadataDatabaseSpecific metadataDatabaseSpecific;

    @JsonProperty("required")
    public boolean required;

    public interface MetadataDatabaseSpecific
    {}
}
