/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.otelmetrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.linecorp.armeria.client.Clients;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.GrpcBasicAuthenticationProvider;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OTelMetricsSourceAcknowledgementsTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final String GRPC_ENDPOINT = "gproto+http://127.0.0.1:21891/";
    private static final int REQUEST_TIMEOUT_MILLIS = 5000;
    private static final long TIMEOUT_SECONDS = 10;

    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private GrpcBasicAuthenticationProvider authenticationProvider;
    @Mock
    private Buffer<Record<? extends Metric>> buffer;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private PipelineDescription pipelineDescription;

    private final CopyOnWriteArrayList<Consumer<Boolean>> callbacks = new CopyOnWriteArrayList<>();
    private PluginMetrics pluginMetrics;
    private OTelMetricsSource objectUnderTest;

    @BeforeEach
    void setUp() {
        pluginMetrics = PluginMetrics.fromNames(UUID.randomUUID().toString(), UUID.randomUUID().toString());
    }

    @AfterEach
    void tearDown() {
        if (objectUnderTest != null) {
            callbacks.forEach(callback -> callback.accept(true));
            objectUnderTest.stop();
        }
    }

    private static OTelMetricsSourceConfig createConfig(final boolean acknowledgments, final int requestTimeoutMillis) {
        return OBJECT_MAPPER.convertValue(Map.of(
                OTelMetricsSourceConfig.SSL, false,
                OTelMetricsSourceConfig.ACKNOWLEDGMENTS, acknowledgments,
                OTelMetricsSourceConfig.REQUEST_TIMEOUT, requestTimeoutMillis), OTelMetricsSourceConfig.class);
    }

    private void startObjectUnderTest(final int requestTimeoutMillis) {
        when(pipelineDescription.getPipelineName()).thenReturn(UUID.randomUUID().toString());
        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(authenticationProvider);
        when(acknowledgementSetManager.create(any(), any(Duration.class), anyBoolean())).thenAnswer(invocation -> {
            callbacks.add(invocation.getArgument(0));
            return mock(AcknowledgementSet.class);
        });
        objectUnderTest = new OTelMetricsSource(createConfig(true, requestTimeoutMillis), pluginMetrics, pluginFactory, pipelineDescription,
                acknowledgementSetManager);
        objectUnderTest.start(buffer);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void acknowledgement_flags_follow_the_configuration(final boolean acknowledgments) {
        when(pipelineDescription.getPipelineName()).thenReturn(UUID.randomUUID().toString());
        final OTelMetricsSource source = new OTelMetricsSource(createConfig(acknowledgments, REQUEST_TIMEOUT_MILLIS), pluginMetrics, pluginFactory,
                pipelineDescription, acknowledgementSetManager);

        assertThat(source.areAcknowledgementsEnabled(), equalTo(acknowledgments));
        assertThat(source.requiresStrictAcknowledgementValidation(), equalTo(acknowledgments));
    }

    @Test
    void constructor_with_acknowledgments_enabled_and_no_acknowledgement_set_manager_throws() {
        final OTelMetricsSourceConfig config = createConfig(true, REQUEST_TIMEOUT_MILLIS);

        assertThrows(NullPointerException.class, () -> new OTelMetricsSource(config, pluginMetrics, pluginFactory, pipelineDescription));
    }

    @Test
    void grpc_request_is_answered_only_after_positive_acknowledgement() throws Exception {
        startObjectUnderTest(REQUEST_TIMEOUT_MILLIS);

        final CompletableFuture<ExportMetricsServiceResponse> response = sendRequest();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(callbacks, hasSize(1)));
        assertThat(response.isDone(), equalTo(false));

        callbacks.get(0).accept(true);

        assertThat(response.get(TIMEOUT_SECONDS, TimeUnit.SECONDS), notNullValue());
        verify(buffer).writeAll(any(), anyInt());
    }

    @Test
    void grpc_request_with_negative_acknowledgement_returns_unavailable() throws Exception {
        startObjectUnderTest(REQUEST_TIMEOUT_MILLIS);

        final CompletableFuture<ExportMetricsServiceResponse> response = sendRequest();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(callbacks, hasSize(1)));

        callbacks.get(0).accept(false);

        assertThat(getStatusCode(response), equalTo(Status.Code.UNAVAILABLE));
    }

    @Test
    void grpc_request_without_acknowledgement_before_request_timeout_returns_resource_exhausted() throws Exception {
        startObjectUnderTest(500);

        assertThat(getStatusCode(sendRequest()), equalTo(Status.Code.RESOURCE_EXHAUSTED));
    }

    @Test
    void stop_waits_for_pending_request_and_rejects_new_requests() throws Exception {
        startObjectUnderTest(REQUEST_TIMEOUT_MILLIS);
        final CompletableFuture<ExportMetricsServiceResponse> pendingResponse = sendRequest();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(callbacks, hasSize(1)));

        final OTelMetricsSource stopping = objectUnderTest;
        objectUnderTest = null;
        final Thread stopThread = new Thread(stopping::stop);
        stopThread.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> stopThread.getState() == Thread.State.TIMED_WAITING);

        assertThat(getStatusCode(sendRequest()), equalTo(Status.Code.UNAVAILABLE));
        assertThat(callbacks, hasSize(1));
        assertThat(stopThread.isAlive(), equalTo(true));

        callbacks.get(0).accept(true);

        assertThat(pendingResponse.get(TIMEOUT_SECONDS, TimeUnit.SECONDS), notNullValue());
        await().atMost(Duration.ofSeconds(TIMEOUT_SECONDS)).until(() -> !stopThread.isAlive());
    }

    private static Status.Code getStatusCode(final CompletableFuture<ExportMetricsServiceResponse> response) {
        final ExecutionException exception = assertThrows(ExecutionException.class, () -> response.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertThat(exception.getCause(), instanceOf(StatusRuntimeException.class));
        return ((StatusRuntimeException) exception.getCause()).getStatus().getCode();
    }

    private static CompletableFuture<ExportMetricsServiceResponse> sendRequest() {
        final MetricsServiceGrpc.MetricsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .responseTimeoutMillis(30000)
                .build(MetricsServiceGrpc.MetricsServiceBlockingStub.class);
        return CompletableFuture.supplyAsync(() -> client.export(createRequest()));
    }

    private static ExportMetricsServiceRequest createRequest() {
        final ScopeMetrics.Builder scopeMetrics = ScopeMetrics.newBuilder();
        scopeMetrics.addMetricsBuilder()
                .setName(UUID.randomUUID().toString())
                .setUnit("seconds")
                .setGauge(Gauge.newBuilder().addDataPoints(NumberDataPoint.newBuilder().setAsInt(4)));
        return ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(ResourceMetrics.newBuilder().addScopeMetrics(scopeMetrics))
                .build();
    }
}
