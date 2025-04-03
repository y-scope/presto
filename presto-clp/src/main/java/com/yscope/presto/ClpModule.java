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
package com.yscope.presto;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.spi.PrestoException;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import com.yscope.presto.metadata.ClpMetadataProvider;
import com.yscope.presto.metadata.ClpMySQLMetadataProvider;
import com.yscope.presto.split.ClpMySQLSplitProvider;
import com.yscope.presto.split.ClpSplitProvider;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;

public class ClpModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(ClpConnector.class).in(Scopes.SINGLETON);
        binder.bind(ClpMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ClpSplitManager.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ClpConfig.class);

        ClpConfig config = buildConfigObject(ClpConfig.class);
        if (config.getMetadataProviderType() == ClpConfig.MetadataProviderType.MYSQL) {
            binder.bind(ClpMetadataProvider.class).to(ClpMySQLMetadataProvider.class).in(Scopes.SINGLETON);
        }
        else {
            throw new PrestoException(ClpErrorCode.CLP_UNSUPPORTED_METADATA_SOURCE,
                    "Unsupported metadata provider type: " + config.getMetadataProviderType());
        }

        if (config.getSplitProviderType() == ClpConfig.SplitProviderType.MYSQL) {
            binder.bind(ClpSplitProvider.class).to(ClpMySQLSplitProvider.class).in(Scopes.SINGLETON);
        }
        else {
            throw new PrestoException(ClpErrorCode.CLP_UNSUPPORTED_SPLIT_SOURCE,
                    "Unsupported split provider type: " + config.getSplitProviderType());
        }
    }
}
