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

import com.facebook.airlift.configuration.Config;

public class ClpConfig
{
    // TODO(Rui): We also need to change it in Velox and in the example configuration files
    public enum ArchiveSource
    {
        LOCAL,
        S3
    }

    public enum MetadataSource
    {
        MYSQL
    }

    // TODO(Rui): come up with a better name
    public enum SplitSource
    {
        MYSQL
    }

    private boolean polymorphicTypeEnabled = true;
    private MetadataSource metadataSource = MetadataSource.MYSQL;
    // TODO(Rui): We need to change it in the example configuration files and in Velox
    private String metadataDbUrl;
    private String metadataDbName;
    private String metadataDbUser;
    private String metadataDbPassword;
    private String metadataTablePrefix;
    private long metadataRefreshInterval = 60;
    private long metadataExpireInterval = 600;
    private ArchiveSource archiveSource = ArchiveSource.LOCAL;
    // TODO(Rui): We need to add it in the example configuration files and in Velox
    private SplitSource splitSource = SplitSource.MYSQL;
    private String clpArchiveDir;
    private String s3Bucket;
    private String s3KeyPrefix;

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

    public MetadataSource getMetadataSource()
    {
        return metadataSource;
    }

    @Config("clp.metadata-source")
    public ClpConfig setMetadataSource(MetadataSource metadataSource)
    {
        this.metadataSource = metadataSource;
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

    public ArchiveSource getInputSource()
    {
        return archiveSource;
    }

    @Config("clp.input-source")
    public ClpConfig setInputSource(ArchiveSource archiveSource)
    {
        this.archiveSource = archiveSource;
        return this;
    }

    public SplitSource getSplitSource()
    {
        return splitSource;
    }

    @Config("clp.split-source")
    public ClpConfig setSplitSource(SplitSource splitSource)
    {
        this.splitSource = splitSource;
        return this;
    }

    public String getClpArchiveDir()
    {
        return clpArchiveDir;
    }

    @Config("clp.archive-dir")
    public ClpConfig setClpArchiveDir(String clpArchiveDir)
    {
        this.clpArchiveDir = clpArchiveDir;
        return this;
    }

    public String getS3Bucket()
    {
        return s3Bucket;
    }

    @Config("clp.s3-bucket")
    public ClpConfig setS3Bucket(String s3Bucket)
    {
        this.s3Bucket = s3Bucket;
        return this;
    }

    public String getS3KeyPrefix()
    {
        return s3KeyPrefix;
    }

    @Config("clp.s3-key-prefix")
    public ClpConfig setS3KeyPrefix(String s3KeyPrefix)
    {
        this.s3KeyPrefix = s3KeyPrefix;
        return this;
    }
}
