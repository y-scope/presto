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
package com.facebook.presto.plugin.clp.metadata.filter;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

import static com.facebook.presto.plugin.clp.metadata.filter.ClpMetadataFilter.MetadataDatabaseSpecific;

/**
 * Uses the given implementation of {@link MetadataDatabaseSpecific} to deserialize the
 * {@code "metadataDatabaseSpecific"} field in the filter. The implementation is originally chosen
 * by the {@code clp.metadata-provider-type} config option.
 */
public class ClpMetadataDatabaseSpecificDeserializer
        extends JsonDeserializer<MetadataDatabaseSpecific>
{
    private final Class<? extends MetadataDatabaseSpecific> actualMetadataDatabaseSpecificClass;

    public ClpMetadataDatabaseSpecificDeserializer(Class<? extends MetadataDatabaseSpecific> actualMetadataDatabaseSpecificClass)
    {
        this.actualMetadataDatabaseSpecificClass = actualMetadataDatabaseSpecificClass;
    }

    @Override
    public MetadataDatabaseSpecific deserialize(JsonParser p, DeserializationContext ctxt) throws IOException
    {
        ObjectNode node = p.getCodec().readTree(p);
        ObjectMapper mapper = (ObjectMapper) p.getCodec();

        return mapper.treeToValue(node, actualMetadataDatabaseSpecificClass);
    }
}
