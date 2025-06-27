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
package com.facebook.presto.plugin.clp;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.plugin.clp.metadata.ClpSchemaTree;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;

public class ClpUtils
{
    private static final Logger log = Logger.get(ClpUtils.class);

    private ClpUtils()
    {
    }

    /**
     * Refer to
     * <a href="https://docs.yscope.com/clp/main/user-guide/reference-json-search-syntax">here
     * </a> for all special chars in the string value that need to be escaped.
     *
     * @param literalString the target string to escape special chars '\', '"', '?' and '*'
     * @return the escaped string
     */
    public static String escapeKqlSpecialCharsForStringValue(String literalString)
    {
        String escaped = literalString;
        escaped = escaped.replace("\\", "\\\\");
        escaped = escaped.replace("\"", "\\\"");
        escaped = escaped.replace("?", "\\?");
        escaped = escaped.replace("*", "\\*");
        return escaped;
    }

    /**
     * Check if the type is one of the numeric types that CLP will handle. Refer to
     * {@link ClpSchemaTree} for all types that CLP will handle.
     *
     * @param type the type to check
     * @return is the type numeric or not
     */
    public static boolean isNumericType(Type type)
    {
        return type.equals(BIGINT)
                || type.equals(INTEGER)
                || type.equals(SMALLINT)
                || type.equals(TINYINT)
                || type.equals(DOUBLE)
                || type.equals(REAL)
                || type instanceof DecimalType;
    }
}
