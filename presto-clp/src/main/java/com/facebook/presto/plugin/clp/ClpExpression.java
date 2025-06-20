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

import com.facebook.presto.spi.relation.RowExpression;

import java.util.Optional;

/**
 * Represents the result of converting a Presto RowExpression into a CLP-compatible KQL query.
 * There are three possible cases:
 * 1. The entire RowExpression is convertible to KQL: `definition` is set, `remainingExpression` is empty.
 * 2. Part of the RowExpression is convertible: the KQL part is stored in `definition`,
 *    and the remaining untranslatable part is stored in `remainingExpression`.
 * 3. None of the expression is convertible: the full RowExpression is stored in `remainingExpression`,
 *    and `definition` is empty.
 */
public class ClpExpression
{
    // Optional KQL query string representing the fully or partially translatable part of the expression.
    private final Optional<String> definition;
    // Optinal SQL string extracted from the definition, which is only made of given metadata columns.
    private final Optional<String> metadataSql;

    // The remaining (non-translatable) portion of the RowExpression, if any.
    private final Optional<RowExpression> remainingExpression;

    public ClpExpression(String definition, String metadataSql, RowExpression remainingExpression)
    {
        this.definition = Optional.ofNullable(definition);
        this.metadataSql = Optional.ofNullable(metadataSql);
        this.remainingExpression = Optional.ofNullable(remainingExpression);
    }

    // Creates an empty ClpExpression (no KQL definition, no remaining expression).
    public ClpExpression()
    {
        this (null, null, null);
    }

    // Creates a ClpExpression from a fully translatable KQL string.
    public ClpExpression(String definition)
    {
        this(definition, null, null);
    }

    // Creates a ClpExpression from a fully translatable KQL string and give it metadata SQL.
    public ClpExpression(String definition, String metadataSql)
    {
        this(definition, metadataSql, null);
    }

    // Creates a ClpExpression from a non-translatable RowExpression.
    public ClpExpression(RowExpression remainingExpression)
    {
        this(null, null, remainingExpression);
    }

    public Optional<String> getDefinition()
    {
        return definition;
    }

    public Optional<String> getMetadataSql()
    {
        return metadataSql;
    }

    public Optional<RowExpression> getRemainingExpression()
    {
        return remainingExpression;
    }
}
