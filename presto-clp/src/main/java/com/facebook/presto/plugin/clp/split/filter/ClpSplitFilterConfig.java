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
package com.facebook.presto.plugin.clp.split.filter;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Defines the basic filter JSON structure.
 * <p></p>
 * Here are the explanations for each field:
 * <ul>
 *   <li><b>{@code columnName}</b>: the data column's name.</li>
 *
 *   <li><b>{@code customOptions}</b>: the split filter provider specific sub-object.</li>
 *
 *   <li><b>{@code required}</b> (optional, defaults to {@code false}): indicates whether the
 *   filter must be present in the pushed-down expression for split filtering. If a required
 *   filter is missing or cannot be pushed down, the query will be rejected.</li>
 * </ul>
 */
public class ClpSplitFilterConfig
{
    @JsonProperty("columnName")
    public String columnName;

    @JsonProperty("customOptions")
    public CustomSplitFilterOptions customOptions;

    @JsonProperty("required")
    public boolean required;

    public interface CustomSplitFilterOptions
    {}
}
