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

import com.facebook.airlift.configuration.Config;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.regex.Pattern;

public class ClpConfig
{
    public static final Pattern SAFE_SQL_TABLE_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9_]+$");

    private boolean polymorphicTypeEnabled = true;

    private MetadataProviderType metadataProviderType = MetadataProviderType.MYSQL;
    private String metadataDbUrl;
    private String metadataDbName;
    private String metadataDbUser;
    private String metadataDbPassword;
    private String metadataTablePrefix;
    private long metadataRefreshInterval = 60;
    private long metadataExpireInterval = 600;

    private String splitMetadataConfigPath;
    private String metadataYamlPath;
    private SplitProviderType splitProviderType = SplitProviderType.MYSQL;
    private String customStorageBaseUrl;
    private Map<String, String> customHttpHeaders = ImmutableMap.of();
    private String customTableNamePrefix;
    private String customApiEndpointPath;

    public boolean isPolymorphicTypeEnabled()
    {
        return polymorphicTypeEnabled;
    }

    @Config("clp.polymorphic-type-enabled")
    public ClpConfig setPolymorphicTypeEnabled(boolean polymorphicTypeEnabled)
    {
        this.polymorphicTypeEnabled = polymorphicTypeEnabled;
        return this;
    }

    public MetadataProviderType getMetadataProviderType()
    {
        return metadataProviderType;
    }

    @Config("clp.metadata-provider-type")
    public ClpConfig setMetadataProviderType(MetadataProviderType metadataProviderType)
    {
        this.metadataProviderType = metadataProviderType;
        return this;
    }

    public String getMetadataDbUrl()
    {
        return metadataDbUrl;
    }

    @Config("clp.metadata-db-url")
    public ClpConfig setMetadataDbUrl(String metadataDbUrl)
    {
        this.metadataDbUrl = metadataDbUrl;
        return this;
    }

    public String getMetadataDbName()
    {
        return metadataDbName;
    }

    @Config("clp.metadata-db-name")
    public ClpConfig setMetadataDbName(String metadataDbName)
    {
        this.metadataDbName = metadataDbName;
        return this;
    }

    public String getMetadataDbUser()
    {
        return metadataDbUser;
    }

    @Config("clp.metadata-db-user")
    public ClpConfig setMetadataDbUser(String metadataDbUser)
    {
        this.metadataDbUser = metadataDbUser;
        return this;
    }

    public String getMetadataDbPassword()
    {
        return metadataDbPassword;
    }

    @Config("clp.metadata-db-password")
    public ClpConfig setMetadataDbPassword(String metadataDbPassword)
    {
        this.metadataDbPassword = metadataDbPassword;
        return this;
    }

    public String getMetadataTablePrefix()
    {
        return metadataTablePrefix;
    }

    @Config("clp.metadata-table-prefix")
    public ClpConfig setMetadataTablePrefix(String metadataTablePrefix)
    {
        if (metadataTablePrefix == null || !SAFE_SQL_TABLE_NAME_PATTERN.matcher(metadataTablePrefix).matches()) {
            throw new PrestoException(
                    ClpErrorCode.CLP_UNSUPPORTED_CONFIG_OPTION,
                    "Invalid metadataTablePrefix: " + metadataTablePrefix + ". Only alphanumeric characters and underscores are allowed.");
        }

        this.metadataTablePrefix = metadataTablePrefix;
        return this;
    }

    public long getMetadataRefreshInterval()
    {
        return metadataRefreshInterval;
    }

    @Config("clp.metadata-refresh-interval")
    public ClpConfig setMetadataRefreshInterval(long metadataRefreshInterval)
    {
        this.metadataRefreshInterval = metadataRefreshInterval;
        return this;
    }

    public long getMetadataExpireInterval()
    {
        return metadataExpireInterval;
    }

    @Config("clp.metadata-expire-interval")
    public ClpConfig setMetadataExpireInterval(long metadataExpireInterval)
    {
        this.metadataExpireInterval = metadataExpireInterval;
        return this;
    }

    public String getMetadataYamlPath()
    {
        return metadataYamlPath;
    }

    @Config("clp.metadata-yaml-path")
    public ClpConfig setMetadataYamlPath(String metadataYamlPath)
    {
        this.metadataYamlPath = metadataYamlPath;
        return this;
    }

    public String getSplitMetadataConfigPath()
    {
        return splitMetadataConfigPath;
    }

    @Config("clp.split-metadata-config-path")
    public ClpConfig setSplitMetadataConfigPath(String splitMetadataConfigPath)
    {
        this.splitMetadataConfigPath = splitMetadataConfigPath;
        return this;
    }

    public SplitProviderType getSplitProviderType()
    {
        return splitProviderType;
    }

    @Config("clp.split-provider-type")
    public ClpConfig setSplitProviderType(SplitProviderType splitProviderType)
    {
        this.splitProviderType = splitProviderType;
        return this;
    }

    public String getCustomStorageBaseUrl()
    {
        return customStorageBaseUrl;
    }

    @Config("clp.custom-metadata-storage-base-url")
    public ClpConfig setCustomStorageBaseUrl(String customStorageBaseUrl)
    {
        this.customStorageBaseUrl = customStorageBaseUrl;
        return this;
    }

    public Map<String, String> getCustomHttpHeaders()
    {
        return customHttpHeaders;
    }

    /**
     * Sets custom HTTP headers for the custom Pinot split provider.
     * <p>
     * Format: comma-separated key:value pairs, e.g., "Header1:Value1,Header2:Value2"
     * </p>
     */
    @Config("clp.custom-metadata-http-headers")
    public ClpConfig setCustomHttpHeaders(String customHttpHeaders)
    {
        if (customHttpHeaders == null || customHttpHeaders.trim().isEmpty()) {
            this.customHttpHeaders = ImmutableMap.of();
            return this;
        }

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        for (String pair : customHttpHeaders.split(",")) {
            String trimmedPair = pair.trim();
            if (trimmedPair.isEmpty()) {
                continue;
            }
            int colonIndex = trimmedPair.indexOf(':');
            if (colonIndex <= 0 || colonIndex >= trimmedPair.length() - 1) {
                throw new PrestoException(
                        ClpErrorCode.CLP_UNSUPPORTED_CONFIG_OPTION,
                        "Invalid custom HTTP header format: '" + trimmedPair + "'. Expected format: 'Header-Name:Header-Value'");
            }
            String key = trimmedPair.substring(0, colonIndex).trim();
            String value = trimmedPair.substring(colonIndex + 1).trim();
            builder.put(key, value);
        }
        this.customHttpHeaders = builder.build();
        return this;
    }

    public String getCustomTableNamePrefix()
    {
        return customTableNamePrefix;
    }

    @Config("clp.custom-metadata-table-name-prefix")
    public ClpConfig setCustomTableNamePrefix(String customTableNamePrefix)
    {
        this.customTableNamePrefix = customTableNamePrefix;
        return this;
    }

    public String getCustomApiEndpointPath()
    {
        return customApiEndpointPath;
    }

    @Config("clp.custom-metadata-api-endpoint-path")
    public ClpConfig setCustomApiEndpointPath(String customApiEndpointPath)
    {
        this.customApiEndpointPath = customApiEndpointPath;
        return this;
    }

    public enum MetadataProviderType
    {
        MYSQL,
        YAML
    }

    public enum SplitProviderType
    {
        MYSQL,
        PINOT,
        PINOT_CUSTOM
    }
}
