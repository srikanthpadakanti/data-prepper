/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.otlp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.protobuf.ByteString;
import com.linecorp.armeria.client.Clients;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
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
class OTLPSourceAcknowledgementsTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final String GRPC_ENDPOINT = "gproto+http://127.0.0.1:21893/";
    private static final int REQUEST_TIMEOUT_MILLIS = 5000;
    private static final long TIMEOUT_SECONDS = 10;

    enum Signal {
        LOGS, METRICS, TRACES
    }

    @Mock
    private PluginFactory pluginFactory;
    @Mock
    private GrpcBasicAuthenticationProvider authenticationProvider;
    @Mock
    private Buffer<Record<Object>> buffer;
    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private PipelineDescription pipelineDescription;

    private final CopyOnWriteArrayList<Consumer<Boolean>> callbacks = new CopyOnWriteArrayList<>();
    private PluginMetrics pluginMetrics;
    private OTLPSource objectUnderTest;

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

    private static OTLPSourceConfig createConfig(final boolean acknowledgments, final int requestTimeoutMillis) {
        return OBJECT_MAPPER.convertValue(Map.of(
                OTLPSourceConfig.SSL, false,
                OTLPSourceConfig.ACKNOWLEDGMENTS, acknowledgments,
                OTLPSourceConfig.REQUEST_TIMEOUT, requestTimeoutMillis), OTLPSourceConfig.class);
    }

    private void startObjectUnderTest(final int requestTimeoutMillis) {
        when(pipelineDescription.getPipelineName()).thenReturn(UUID.randomUUID().toString());
        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(authenticationProvider);
        when(acknowledgementSetManager.create(any(), any(Duration.class), anyBoolean())).thenAnswer(invocation -> {
            callbacks.add(invocation.getArgument(0));
            return mock(AcknowledgementSet.class);
        });
        objectUnderTest = new OTLPSource(createConfig(true, requestTimeoutMillis), pluginMetrics, pluginFactory, pipelineDescription,
                acknowledgementSetManager);
        objectUnderTest.start(buffer);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void acknowledgement_flags_follow_the_configuration(final boolean acknowledgments) {
        when(pipelineDescription.getPipelineName()).thenReturn(UUID.randomUUID().toString());
        final OTLPSource source = new OTLPSource(createConfig(acknowledgments, REQUEST_TIMEOUT_MILLIS), pluginMetrics, pluginFactory,
                pipelineDescription, acknowledgementSetManager);

        assertThat(source.areAcknowledgementsEnabled(), equalTo(acknowledgments));
        assertThat(source.requiresStrictAcknowledgementValidation(), equalTo(acknowledgments));
    }

    @Test
    void constructor_with_acknowledgments_enabled_and_no_acknowledgement_set_manager_throws() {
        final OTLPSourceConfig config = createConfig(true, REQUEST_TIMEOUT_MILLIS);

        assertThrows(NullPointerException.class, () -> new OTLPSource(config, pluginMetrics, pluginFactory, pipelineDescription));
    }

    @ParameterizedTest
    @EnumSource(Signal.class)
    void grpc_request_is_answered_only_after_positive_acknowledgement(final Signal signal) throws Exception {
        startObjectUnderTest(REQUEST_TIMEOUT_MILLIS);

        final CompletableFuture<Object> response = sendRequest(signal);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(callbacks, hasSize(1)));
        assertThat(response.isDone(), equalTo(false));

        callbacks.get(0).accept(true);

        assertThat(response.get(TIMEOUT_SECONDS, TimeUnit.SECONDS), notNullValue());
        verify(buffer).writeAll(any(), anyInt());
    }

    @ParameterizedTest
    @EnumSource(Signal.class)
    void grpc_request_with_negative_acknowledgement_returns_unavailable(final Signal signal) throws Exception {
        startObjectUnderTest(REQUEST_TIMEOUT_MILLIS);

        final CompletableFuture<Object> response = sendRequest(signal);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(callbacks, hasSize(1)));

        callbacks.get(0).accept(false);

        assertThat(getStatusCode(response), equalTo(Status.Code.UNAVAILABLE));
    }

    @Test
    void stop_waits_for_pending_request_and_rejects_new_requests() throws Exception {
        startObjectUnderTest(REQUEST_TIMEOUT_MILLIS);
        final CompletableFuture<Object> pendingResponse = sendRequest(Signal.TRACES);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(callbacks, hasSize(1)));

        final OTLPSource stopping = objectUnderTest;
        objectUnderTest = null;
        final Thread stopThread = new Thread(stopping::stop);
        stopThread.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> stopThread.getState() == Thread.State.TIMED_WAITING);

        assertThat(getStatusCode(sendRequest(Signal.LOGS)), equalTo(Status.Code.UNAVAILABLE));
        assertThat(callbacks, hasSize(1));
        assertThat(stopThread.isAlive(), equalTo(true));

        callbacks.get(0).accept(true);

        assertThat(pendingResponse.get(TIMEOUT_SECONDS, TimeUnit.SECONDS), notNullValue());
        await().atMost(Duration.ofSeconds(TIMEOUT_SECONDS)).until(() -> !stopThread.isAlive());
    }

    private static Status.Code getStatusCode(final CompletableFuture<Object> response) {
        final ExecutionException exception = assertThrows(ExecutionException.class, () -> response.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertThat(exception.getCause(), instanceOf(StatusRuntimeException.class));
        return ((StatusRuntimeException) exception.getCause()).getStatus().getCode();
    }

    private static CompletableFuture<Object> sendRequest(final Signal signal) {
        switch (signal) {
            case LOGS:
                final LogsServiceGrpc.LogsServiceBlockingStub logsClient = Clients.builder(GRPC_ENDPOINT)
                        .responseTimeoutMillis(30000)
                        .build(LogsServiceGrpc.LogsServiceBlockingStub.class);
                return CompletableFuture.supplyAsync(() -> logsClient.export(createLogsRequest()));
            case METRICS:
                final MetricsServiceGrpc.MetricsServiceBlockingStub metricsClient = Clients.builder(GRPC_ENDPOINT)
                        .responseTimeoutMillis(30000)
                        .build(MetricsServiceGrpc.MetricsServiceBlockingStub.class);
                return CompletableFuture.supplyAsync(() -> metricsClient.export(createMetricsRequest()));
            default:
                final TraceServiceGrpc.TraceServiceBlockingStub tracesClient = Clients.builder(GRPC_ENDPOINT)
                        .responseTimeoutMillis(30000)
                        .build(TraceServiceGrpc.TraceServiceBlockingStub.class);
                return CompletableFuture.supplyAsync(() -> tracesClient.export(createTracesRequest()));
        }
    }

    private static ExportLogsServiceRequest createLogsRequest() {
        return ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(ResourceLogs.newBuilder()
                        .addScopeLogs(ScopeLogs.newBuilder()
                                .addLogRecords(LogRecord.newBuilder()
                                        .setBody(AnyValue.newBuilder().setStringValue(UUID.randomUUID().toString()).build()))))
                .build();
    }

    private static ExportMetricsServiceRequest createMetricsRequest() {
        final ScopeMetrics.Builder scopeMetrics = ScopeMetrics.newBuilder();
        scopeMetrics.addMetricsBuilder()
                .setName(UUID.randomUUID().toString())
                .setUnit("seconds")
                .setGauge(Gauge.newBuilder().addDataPoints(NumberDataPoint.newBuilder().setAsInt(4)));
        return ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(ResourceMetrics.newBuilder().addScopeMetrics(scopeMetrics))
                .build();
    }

    private static ExportTraceServiceRequest createTracesRequest() {
        return ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(ResourceSpans.newBuilder()
                        .addScopeSpans(ScopeSpans.newBuilder()
                                .addSpans(Span.newBuilder()
                                        .setTraceId(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                                        .setSpanId(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                                        .setName(UUID.randomUUID().toString())
                                        .setKind(Span.SpanKind.SPAN_KIND_SERVER)
                                        .setStartTimeUnixNano(100)
                                        .setEndTimeUnixNano(101))))
                .build();
    }
}
