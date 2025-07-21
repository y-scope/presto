package com.facebook.presto.plugin.clp.metadata.filter;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

import static com.facebook.presto.plugin.clp.metadata.filter.ClpMetadataFilter.MetadataDatabaseSpecific;

public class ClpMetadataDatabaseSpecificDeserializer extends JsonDeserializer<MetadataDatabaseSpecific>
{
    private final Class<? extends MetadataDatabaseSpecific> actualMetadataDatabaseSpecificClass;

    public ClpMetadataDatabaseSpecificDeserializer(Class<? extends MetadataDatabaseSpecific> actualMetadataDatabaseSpecificClass)
    {
        this.actualMetadataDatabaseSpecificClass = actualMetadataDatabaseSpecificClass;
    }

    @Override
    public MetadataDatabaseSpecific deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        ObjectNode node = p.getCodec().readTree(p);
        ObjectMapper mapper = (ObjectMapper) p.getCodec();

        return mapper.treeToValue(node, actualMetadataDatabaseSpecificClass);
    }
}
