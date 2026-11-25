/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import java.time.Duration;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HTTPSourceConfigTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Test
    void testDefault() {
        // Prepare
        final HTTPSourceConfig sourceConfig = new HTTPSourceConfig();

        // When/Then
        assertEquals(HTTPSourceConfig.DEFAULT_PORT, sourceConfig.getPort());
        assertEquals(HTTPSourceConfig.DEFAULT_LOG_INGEST_URI, sourceConfig.getPath());
        assertEquals(HTTPSourceConfig.DEFAULT_PORT, sourceConfig.getDefaultPort());
        assertEquals(HTTPSourceConfig.DEFAULT_LOG_INGEST_URI, sourceConfig.getDefaultPath());
        assertEquals(sourceConfig.getMetadataHeaders(), Collections.emptyList());
    }

    @Test
    void testSetMetadataHeaders() throws Exception {
        final String json = "{\"metadata_headers\": [\"X-Tenant-Id\", \"X-Region\"]}";
        final HTTPSourceConfig sourceConfig = OBJECT_MAPPER.readValue(json, HTTPSourceConfig.class);

        assertEquals(List.of("X-Tenant-Id", "X-Region"), sourceConfig.getMetadataHeaders());
    }

    @Test
    void acknowledgments_are_disabled_by_default_with_default_retry_info() {
        final HTTPSourceConfig sourceConfig = new HTTPSourceConfig();

        assertThat(sourceConfig.isAcknowledgmentsEnabled(), equalTo(false));
        assertThat(sourceConfig.getRetryInfo().getMinDelay(), equalTo(Duration.ofMillis(100)));
    }

    @Test
    void acknowledgments_and_retry_info_are_read_from_the_configuration() throws Exception {
        final HTTPSourceConfig sourceConfig = new ObjectMapper().registerModule(new JavaTimeModule()).readValue(
                "{\"acknowledgments\": true, \"retry_info\": {\"min_delay\": \"PT1S\", \"max_delay\": \"PT4S\"}}",
                HTTPSourceConfig.class);

        assertThat(sourceConfig.isAcknowledgmentsEnabled(), equalTo(true));
        assertThat(sourceConfig.getRetryInfo().getMaxDelay(), equalTo(Duration.ofSeconds(4)));
    }
}
