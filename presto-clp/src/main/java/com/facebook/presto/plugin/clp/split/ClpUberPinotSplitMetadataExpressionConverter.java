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

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;

import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION;
import static java.lang.String.format;

/**
 * A converter that converts Presto {@link RowExpression} trees representing metadata predicates
 * into SQL filter strings that can be pushed down to Pinot for split-level metadata filtering.
 * <p></p>
 * The converter:
 * <ul>
 *   <li>Handles standard logical and comparison operators (AND, OR, =, >, >=, <, <=, IS NULL).</li>
 *   <li>Supports range-bound rewriting for data columns that have metadata columns representing
 *       lower and upper bounds.</li>
 *   <li>Tracks required columns and throws an exception if any are missing in the filter
 *       expression.</li>
 * </ul>
 */
public class ClpUberPinotSplitMetadataExpressionConverter
        extends ClpMySqlSplitMetadataExpressionConverter
{
    public ClpUberPinotSplitMetadataExpressionConverter(
            FunctionMetadataManager functionManager,
            StandardFunctionResolution functionResolution,
            ClpSplitMetadataConfig metadataConfig,
            SchemaTableName schemaTableName)
    {
        super(functionManager, functionResolution, metadataConfig, schemaTableName);
    }

    @Override
    public String visitCall(CallExpression node, Void context)
    {
        FunctionHandle functionHandle = node.getFunctionHandle();
        if (functionResolution.isNotFunction(functionHandle)) {
            return format("NOT (%s)", node.getArguments().get(0).accept(this, null));
        }

        FunctionMetadata functionMetadata = functionManager.getFunctionMetadata(node.getFunctionHandle());
        Optional<OperatorType> operatorTypeOptional = functionMetadata.getOperatorType();
        if (operatorTypeOptional.isPresent()) {
            OperatorType operatorType = operatorTypeOptional.get();
            if (operatorType == OperatorType.NEGATION) {
                String value = node.getArguments().get(0).accept(this, null);
                return "-" + value;
            }

            if (operatorType.isComparisonOperator() && operatorType != IS_DISTINCT_FROM) {
                String variableName = node.getArguments().get(0).accept(this, null);
                String literalString = node.getArguments().get(1).accept(this, null);

                Type columnType = node.getArguments().get(0).getType();
                Type literalType = node.getArguments().get(1).getType();
                literalString = coerceLiteralToColumnType(literalString, columnType, literalType);

                String rewritten = rewriteComparisonWithBounds(variableName, operatorType, literalString);
                if (rewritten != null) {
                    return rewritten;
                }

                if (operatorType == EQUAL) {
                    if (literalString.startsWith("'") && literalString.endsWith("'")) {
                        literalString = literalString.substring(1, literalString.length() - 1);
                    }

                    return format("TEXT_MATCH(\"%s\", '/%s:%s/')", "__mergedTextIndex", literalString.replace("''", "'"), variableName);
                }
                return format("%s %s %s", variableName, operatorType.getOperator(), literalString);
            }
        }

        throw new PrestoException(CLP_PUSHDOWN_UNSUPPORTED_EXPRESSION, "Unsupported metadata query: " + node);
    }
}
