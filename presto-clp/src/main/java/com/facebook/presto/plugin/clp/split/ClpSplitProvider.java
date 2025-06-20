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

import com.facebook.presto.plugin.clp.ClpSplit;
import com.facebook.presto.plugin.clp.ClpTableLayoutHandle;

import java.util.List;

/**
 * A provider for splits from a CLP dataset.
 */
public interface ClpSplitProvider
{
    /**
     * @param clpTableLayoutHandle the table layout handle
     * @return the list of splits for the specified table layout
     */
    List<ClpSplit> listSplits(ClpTableLayoutHandle clpTableLayoutHandle);
}
