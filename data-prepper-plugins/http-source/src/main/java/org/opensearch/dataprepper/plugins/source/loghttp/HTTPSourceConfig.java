/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import org.opensearch.dataprepper.http.BaseHttpServerConfig;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.server.RetryInfoConfig;

import java.util.Collections;
import java.util.List;

public class HTTPSourceConfig extends BaseHttpServerConfig {

    static final String DEFAULT_LOG_INGEST_URI = "/log/ingest";
    static final int DEFAULT_PORT = 2021;

    @Override
    public int getDefaultPort() {
        return DEFAULT_PORT;
    }

    @Override
    public String getDefaultPath() {
        return DEFAULT_LOG_INGEST_URI;
    }

    @JsonProperty("codec")
    private PluginModel codec;

    @JsonProperty("metadata_headers")
    private List<String> metadataHeaders = Collections.emptyList();

    @JsonProperty("acknowledgments")
    @JsonPropertyDescription("When true, the source responds to a request only after the sinks acknowledge every event in it. Default is false.")
    private boolean acknowledgments = false;

    @JsonProperty("retry_info")
    @JsonPropertyDescription("The delays used for the Retry-After header when acknowledgments are enabled.")
    private RetryInfoConfig retryInfo = new RetryInfoConfig();

    public PluginModel getCodec() {
        return codec;
    }

    public List<String> getMetadataHeaders() {
        return metadataHeaders;
    }

    public boolean isAcknowledgmentsEnabled() {
        return acknowledgments;
    }

    public RetryInfoConfig getRetryInfo() {
        return retryInfo;
    }

}
