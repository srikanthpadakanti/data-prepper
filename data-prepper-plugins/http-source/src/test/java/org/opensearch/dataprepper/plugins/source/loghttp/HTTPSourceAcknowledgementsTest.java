/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBufferConfig;
import org.opensearch.dataprepper.plugins.server.RetryInfoConfig;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class HTTPSourceAcknowledgementsTest {
    private static final int PORT = 2022;
    private static final String PIPELINE_NAME = "acknowledgements-pipeline";
    private static final String BODY = "[{\"log\": \"somelog\"}]";

    private HTTPSourceConfig sourceConfig;
    private AcknowledgementSetManager acknowledgementSetManager;
    private final List<Consumer<Boolean>> callbacks = new CopyOnWriteArrayList<>();
    private final List<AcknowledgementSet> acknowledgementSets = new CopyOnWriteArrayList<>();
    private HTTPSource source;

    @BeforeEach
    void setUp() {
        sourceConfig = mock(HTTPSourceConfig.class);
        lenient().when(sourceConfig.getPort()).thenReturn(PORT);
        lenient().when(sourceConfig.getPath()).thenReturn(HTTPSourceConfig.DEFAULT_LOG_INGEST_URI);
        lenient().when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(10_000);
        lenient().when(sourceConfig.getBufferTimeoutInMillis()).thenReturn(8_000);
        lenient().when(sourceConfig.getThreadCount()).thenReturn(4);
        lenient().when(sourceConfig.getMaxConnectionCount()).thenReturn(50);
        lenient().when(sourceConfig.getMaxPendingRequests()).thenReturn(100);
        lenient().when(sourceConfig.getCompression()).thenReturn(CompressionOption.NONE);
        lenient().when(sourceConfig.getRetryInfo()).thenReturn(new RetryInfoConfig());
        when(sourceConfig.isAcknowledgmentsEnabled()).thenReturn(true);

        acknowledgementSetManager = mock(AcknowledgementSetManager.class);
        lenient().when(acknowledgementSetManager.create(any(), any(Duration.class), anyBoolean())).thenAnswer(invocation -> {
            final AcknowledgementSet acknowledgementSet = mock(AcknowledgementSet.class);
            acknowledgementSets.add(acknowledgementSet);
            callbacks.add(invocation.getArgument(0));
            return acknowledgementSet;
        });
    }

    @AfterEach
    void tearDown() {
        if (source != null) {
            callbacks.forEach(callback -> callback.accept(true));
            source.stop();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void acknowledgement_flags_follow_the_configuration(final boolean acknowledgments) {
        when(sourceConfig.isAcknowledgmentsEnabled()).thenReturn(acknowledgments);

        final HTTPSource objectUnderTest = createSource();

        assertThat(objectUnderTest.areAcknowledgementsEnabled(), equalTo(acknowledgments));
        assertThat(objectUnderTest.requiresStrictAcknowledgementValidation(), equalTo(acknowledgments));
    }

    @Test
    void request_is_answered_ok_only_after_positive_acknowledgement() throws Exception {
        startSource();

        final CompletableFuture<AggregatedHttpResponse> response = post();
        await().atMost(Duration.ofSeconds(10)).until(() -> callbacks.size() == 1);

        assertThat(response.isDone(), equalTo(false));
        verify(acknowledgementSets.get(0)).add(any(Event.class));
        verify(acknowledgementSets.get(0)).complete();

        callbacks.get(0).accept(true);

        assertThat(response.get(10, TimeUnit.SECONDS).status(), equalTo(HttpStatus.OK));
    }

    @Test
    void negative_acknowledgement_is_answered_too_many_requests_with_retry_after() throws Exception {
        startSource();

        final CompletableFuture<AggregatedHttpResponse> response = post();
        await().atMost(Duration.ofSeconds(10)).until(() -> callbacks.size() == 1);
        callbacks.get(0).accept(false);

        final AggregatedHttpResponse aggregatedHttpResponse = response.get(10, TimeUnit.SECONDS);
        assertThat(aggregatedHttpResponse.status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
        assertThat(aggregatedHttpResponse.headers().get(HttpHeaderNames.RETRY_AFTER), equalTo("1"));
    }

    @Test
    void request_timeout_while_waiting_is_answered_too_many_requests_with_retry_after() throws Exception {
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(1_000);
        when(sourceConfig.getBufferTimeoutInMillis()).thenReturn(800);
        startSource();

        final AggregatedHttpResponse aggregatedHttpResponse = post().get(10, TimeUnit.SECONDS);

        assertThat(aggregatedHttpResponse.status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
        assertThat(aggregatedHttpResponse.headers().get(HttpHeaderNames.RETRY_AFTER), equalTo("1"));
    }

    @Test
    void request_over_max_pending_requests_is_rejected_with_retry_after() throws Exception {
        when(sourceConfig.getMaxPendingRequests()).thenReturn(1);
        startSource();

        final CompletableFuture<AggregatedHttpResponse> first = post();
        await().atMost(Duration.ofSeconds(10)).until(() -> callbacks.size() == 1);
        final AggregatedHttpResponse second = post().get(10, TimeUnit.SECONDS);

        assertThat(second.status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
        assertThat(second.headers().get(HttpHeaderNames.RETRY_AFTER), equalTo("1"));
        assertThat(callbacks, hasSize(1));

        callbacks.get(0).accept(true);

        assertThat(first.get(10, TimeUnit.SECONDS).status(), equalTo(HttpStatus.OK));
    }

    @Test
    void bad_body_is_answered_bad_request_without_an_acknowledgement_set() throws Exception {
        startSource();

        final AggregatedHttpResponse response = post("not json").get(10, TimeUnit.SECONDS);

        assertThat(response.status(), equalTo(HttpStatus.BAD_REQUEST));
        assertThat(callbacks, hasSize(0));
    }

    @Test
    void stop_waits_for_pending_requests_and_rejects_new_requests() throws Exception {
        startSource();
        final CompletableFuture<AggregatedHttpResponse> pending = post();
        await().atMost(Duration.ofSeconds(10)).until(() -> callbacks.size() == 1);
        final Thread stopThread = new Thread(source::stop);

        stopThread.start();
        await().atMost(Duration.ofSeconds(10)).until(() -> stopThread.getState() == Thread.State.TIMED_WAITING);
        final AggregatedHttpResponse rejected = post().get(10, TimeUnit.SECONDS);

        assertThat(rejected.status(), equalTo(HttpStatus.SERVICE_UNAVAILABLE));
        assertThat(rejected.headers().get(HttpHeaderNames.RETRY_AFTER), equalTo("1"));
        assertThat(stopThread.isAlive(), equalTo(true));

        callbacks.get(0).accept(true);

        assertThat(pending.get(10, TimeUnit.SECONDS).status(), equalTo(HttpStatus.OK));
        stopThread.join(TimeUnit.SECONDS.toMillis(10));
        assertThat(stopThread.isAlive(), equalTo(false));
    }

    @Test
    void source_without_retry_info_uses_the_default_delays() throws Exception {
        when(sourceConfig.getRetryInfo()).thenReturn(null);
        startSource();

        final CompletableFuture<AggregatedHttpResponse> response = post();
        await().atMost(Duration.ofSeconds(10)).until(() -> callbacks.size() == 1);
        callbacks.get(0).accept(false);

        assertThat(response.get(10, TimeUnit.SECONDS).headers().get(HttpHeaderNames.RETRY_AFTER), equalTo("1"));
    }

    @Test
    void source_with_acknowledgments_and_without_acknowledgement_set_manager_throws() {
        acknowledgementSetManager = null;

        assertThrows(NullPointerException.class, this::createSource);
    }

    @Test
    void full_buffer_is_answered_too_many_requests_with_retry_after() throws Exception {
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(5_000);
        when(sourceConfig.getBufferTimeoutInMillis()).thenReturn(200);
        source = createSource();
        source.start(createBuffer(1));

        post();
        await().atMost(Duration.ofSeconds(10)).until(() -> callbacks.size() == 1);
        final AggregatedHttpResponse response = post().get(10, TimeUnit.SECONDS);

        assertThat(response.status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
        assertThat(response.headers().get(HttpHeaderNames.RETRY_AFTER), equalTo("1"));
    }

    @Test
    void retry_after_uses_the_configured_minimum_delay() throws Exception {
        when(sourceConfig.getRetryInfo()).thenReturn(new RetryInfoConfig(Duration.ofSeconds(3), Duration.ofSeconds(10)));
        startSource();

        final CompletableFuture<AggregatedHttpResponse> response = post();
        await().atMost(Duration.ofSeconds(10)).until(() -> callbacks.size() == 1);
        callbacks.get(0).accept(false);

        assertThat(response.get(10, TimeUnit.SECONDS).headers().get(HttpHeaderNames.RETRY_AFTER), equalTo("3"));
    }

    @Test
    void request_that_timed_out_still_counts_against_max_pending_requests_until_its_set_finishes() throws Exception {
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(1_000);
        when(sourceConfig.getBufferTimeoutInMillis()).thenReturn(800);
        when(sourceConfig.getMaxPendingRequests()).thenReturn(1);
        startSource();

        final AggregatedHttpResponse timedOut = post().get(10, TimeUnit.SECONDS);
        final AggregatedHttpResponse rejected = post().get(10, TimeUnit.SECONDS);

        assertThat(timedOut.status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
        assertThat(rejected.status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
        assertThat(callbacks, hasSize(1));
    }

    @Test
    void late_acknowledgement_after_the_timeout_is_not_counted_as_success() throws Exception {
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(1_000);
        when(sourceConfig.getBufferTimeoutInMillis()).thenReturn(800);
        MetricsTestUtil.initMetrics();
        source = createSource();
        final PluginMetrics pluginMetrics = PluginMetrics.fromNames("http", PIPELINE_NAME);
        source.start(createBuffer(10));

        final AggregatedHttpResponse timedOut = post().get(10, TimeUnit.SECONDS);
        callbacks.get(0).accept(true);

        assertThat(timedOut.status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
        assertThat(pluginMetrics.counter(LogHTTPService.SUCCESS_REQUESTS).count(), equalTo(0.0));
    }

    @Test
    void acknowledgement_set_expires_after_twice_the_request_timeout() throws Exception {
        startSource();

        post();
        await().atMost(Duration.ofSeconds(10)).until(() -> callbacks.size() == 1);

        verify(acknowledgementSetManager).create(any(), eq(Duration.ofMillis(20_000)), eq(false));
    }

    private void startSource() throws Exception {
        source = createSource();
        source.start(createBuffer(10));
    }

    private HTTPSource createSource() {
        final PluginFactory pluginFactory = mock(PluginFactory.class);
        lenient().when(pluginFactory.loadPlugin(eq(ArmeriaHttpAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(mock(ArmeriaHttpAuthenticationProvider.class));
        final PipelineDescription pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(PIPELINE_NAME);
        return new HTTPSource(sourceConfig, PluginMetrics.fromNames("http", PIPELINE_NAME), pluginFactory,
                pipelineDescription, acknowledgementSetManager);
    }

    private static BlockingBuffer<Record<Log>> createBuffer(final int bufferSize) throws Exception {
        final ObjectMapper objectMapper = new ObjectMapper();
        final BlockingBufferConfig blockingBufferConfig = objectMapper.readValue(
                objectMapper.writeValueAsString(Map.of("buffer_size", bufferSize, "batch_size", bufferSize)), BlockingBufferConfig.class);
        final PipelineDescription pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(PIPELINE_NAME);
        return new BlockingBuffer<>(blockingBufferConfig, pipelineDescription);
    }

    private static CompletableFuture<AggregatedHttpResponse> post() {
        return post(BODY);
    }

    private static CompletableFuture<AggregatedHttpResponse> post(final String body) {
        return WebClient.of().execute(RequestHeaders.builder()
                        .scheme("http")
                        .authority("127.0.0.1:" + PORT)
                        .method(HttpMethod.POST)
                        .path(HTTPSourceConfig.DEFAULT_LOG_INGEST_URI)
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.ofUtf8(body)).aggregate();
    }
}
