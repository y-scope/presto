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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;

public final class ClpFunctions
{
    private ClpFunctions()
    {
    }

    @ScalarFunction(value = "CLP_GET_INT", deterministic = false)
    @Description("Retrieves an integer value corresponding to the given JSON path.")
    @SqlType(StandardTypes.BIGINT)
    public static long clpGetInt(@SqlType(StandardTypes.VARCHAR) Slice jsonPath)
    {
        return 0;
    }

    @ScalarFunction(value = "CLP_GET_FLOAT", deterministic = false)
    @Description("Retrieves a floating point value corresponding to the given JSON path.")
    @SqlType(StandardTypes.DOUBLE)
    public static double clpGetFloat(@SqlType(StandardTypes.VARCHAR) Slice jsonPath)
    {
        return 0.0;
    }

    @ScalarFunction(value = "CLP_GET_BOOL", deterministic = false)
    @Description("Retrieves a boolean value corresponding to the given JSON path.")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean clpGetBool(@SqlType(StandardTypes.VARCHAR) Slice jsonPath)
    {
        return false;
    }

    @ScalarFunction(value = "CLP_GET_STRING", deterministic = false)
    @Description("Retrieves a string value corresponding to the given JSON path.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice clpGetString(@SqlType(StandardTypes.VARCHAR) Slice jsonPath)
    {
        return Slices.EMPTY_SLICE;
    }

    @ScalarFunction(value = "CLP_GET_STRING_ARRAY", deterministic = false)
    @Description("Retrieves an array value corresponding to the given JSON path and converts each element into a string.")
    @SqlType("ARRAY(VARCHAR)")
    public static Block clpGetStringArray(@SqlType(StandardTypes.VARCHAR) Slice jsonPath)
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 0);
        return blockBuilder.build();
    }

    @ScalarFunction(value = "CLP_WILDCARD_STRING_COLUMN", deterministic = false)
    @Description("Used in filter expressions to allow comparisons with any string column in the log record.")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice clpWildcardStringColumn()
    {
        return Slices.EMPTY_SLICE;
    }

    @ScalarFunction(value = "CLP_WILDCARD_INT_COLUMN", deterministic = false)
    @Description("Used in filter expressions to allow comparisons with any integer column in the log record.")
    @SqlType(StandardTypes.BIGINT)
    public static long clpWildcardIntColumn()
    {
        return 0;
    }

    @ScalarFunction(value = "CLP_WILDCARD_FLOAT_COLUMN", deterministic = false)
    @Description("Used in filter expressions to allow comparisons with any floating point column in the log record.")
    @SqlType(StandardTypes.DOUBLE)
    public static double clpWildcardFloatColumn()
    {
        return 0.0;
    }

    @ScalarFunction(value = "CLP_WILDCARD_BOOL_COLUMN", deterministic = false)
    @Description("Used in filter expressions to allow comparisons with any boolean column in the log record.")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean clpWildcardBoolColumn()
    {
        return false;
    }
}
