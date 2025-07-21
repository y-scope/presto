package com.facebook.presto.plugin.clp.metadata.filter;

import com.facebook.presto.plugin.clp.ClpConfig;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.inject.Inject;

import java.io.IOException;

import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_UNSUPPORTED_METADATA_SOURCE;
import static com.facebook.presto.plugin.clp.ClpConfig.MetadataProviderType;
import static com.facebook.presto.plugin.clp.ClpConfig.MetadataProviderType.MYSQL;
import static com.facebook.presto.plugin.clp.metadata.filter.ClpMetadataFilter.MetadataDatabaseSpecific;

public class ClpMetadataDatabaseSpecificDeserializer extends JsonDeserializer<MetadataDatabaseSpecific>
{
    private final ClpConfig config;

    @Inject
    public ClpMetadataDatabaseSpecificDeserializer(ClpConfig config)
    {
        this.config = config;
    }

    @Override
    public MetadataDatabaseSpecific deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectNode node = p.getCodec().readTree(p);
        MetadataProviderType type = config.getMetadataProviderType();
        ObjectMapper mapper = (ObjectMapper) p.getCodec();

        if (MYSQL == type) {
            return mapper.treeToValue(node, ClpMySqlMetadataFilterProvider.ClpMySqlMetadataDatabaseSpecific.class);
        }
        throw new PrestoException(CLP_UNSUPPORTED_METADATA_SOURCE, "Unsupported metadata provider type: " + type);
    }
}
