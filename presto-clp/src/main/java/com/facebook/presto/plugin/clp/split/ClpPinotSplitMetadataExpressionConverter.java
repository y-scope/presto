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
package com.facebook.presto.plugin.clp.split;

import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;

public class ClpPinotSplitMetadataExpressionConverter
        extends ClpMySqlSplitMetadataExpressionConverter
{
    public ClpPinotSplitMetadataExpressionConverter(
            FunctionMetadataManager functionManager,
            StandardFunctionResolution functionResolution,
            ClpSplitMetadataConfig metadataConfig,
            SchemaTableName schemaTableName)
    {
        super(functionManager, functionResolution, metadataConfig, schemaTableName);
    }
}
