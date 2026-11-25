/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.oteltrace;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
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
class OTelTraceSourceAcknowledgementsTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final String GRPC_ENDPOINT = "gproto+http://127.0.0.1:21890/";
    private static final String HTTP_PATH = "/opentelemetry.proto.collector.trace.v1.TraceService/Export";
    private static final int REQUEST_TIMEOUT_MILLIS = 5000;
    private static final long TIMEOUT_SECONDS = 10;

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
    private OTelTraceSource objectUnderTest;

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

    private static OTelTraceSourceConfig createConfig(final boolean acknowledgments, final int requestTimeoutMillis) {
        return OBJECT_MAPPER.convertValue(Map.of(
                OTelTraceSourceConfig.SSL, false,
                OTelTraceSourceConfig.ACKNOWLEDGMENTS, acknowledgments,
                OTelTraceSourceConfig.REQUEST_TIMEOUT, requestTimeoutMillis), OTelTraceSourceConfig.class);
    }

    private void startObjectUnderTest(final int requestTimeoutMillis) {
        when(pipelineDescription.getPipelineName()).thenReturn(UUID.randomUUID().toString());
        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(authenticationProvider);
        when(acknowledgementSetManager.create(any(), any(Duration.class), anyBoolean())).thenAnswer(invocation -> {
            callbacks.add(invocation.getArgument(0));
            return mock(AcknowledgementSet.class);
        });
        objectUnderTest = new OTelTraceSource(createConfig(true, requestTimeoutMillis), pluginMetrics, pluginFactory, pipelineDescription,
                acknowledgementSetManager);
        objectUnderTest.start(buffer);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void acknowledgement_flags_follow_the_configuration(final boolean acknowledgments) {
        when(pipelineDescription.getPipelineName()).thenReturn(UUID.randomUUID().toString());
        final OTelTraceSource source = new OTelTraceSource(createConfig(acknowledgments, REQUEST_TIMEOUT_MILLIS), pluginMetrics, pluginFactory,
                pipelineDescription, acknowledgementSetManager);

        assertThat(source.areAcknowledgementsEnabled(), equalTo(acknowledgments));
        assertThat(source.requiresStrictAcknowledgementValidation(), equalTo(acknowledgments));
    }

    @Test
    void constructor_with_acknowledgments_enabled_and_no_acknowledgement_set_manager_throws() {
        final OTelTraceSourceConfig config = createConfig(true, REQUEST_TIMEOUT_MILLIS);

        assertThrows(NullPointerException.class, () -> new OTelTraceSource(config, pluginMetrics, pluginFactory, pipelineDescription));
    }

    @Test
    void grpc_request_is_answered_only_after_positive_acknowledgement() throws Exception {
        startObjectUnderTest(REQUEST_TIMEOUT_MILLIS);

        final CompletableFuture<ExportTraceServiceResponse> response = CompletableFuture.supplyAsync(() -> createGrpcClient().export(createRequest()));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(callbacks, hasSize(1)));
        assertThat(response.isDone(), equalTo(false));

        callbacks.get(0).accept(true);

        assertThat(response.get(TIMEOUT_SECONDS, TimeUnit.SECONDS), notNullValue());
        verify(buffer).writeAll(any(), anyInt());
    }

    @Test
    void grpc_request_with_negative_acknowledgement_returns_unavailable() throws Exception {
        startObjectUnderTest(REQUEST_TIMEOUT_MILLIS);

        final CompletableFuture<ExportTraceServiceResponse> response = CompletableFuture.supplyAsync(() -> createGrpcClient().export(createRequest()));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(callbacks, hasSize(1)));

        callbacks.get(0).accept(false);

        final ExecutionException exception = assertThrows(ExecutionException.class, () -> response.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertThat(exception.getCause(), instanceOf(StatusRuntimeException.class));
        assertThat(((StatusRuntimeException) exception.getCause()).getStatus().getCode(), equalTo(Status.Code.UNAVAILABLE));
    }

    @Test
    void http_request_is_answered_only_after_positive_acknowledgement() throws Exception {
        startObjectUnderTest(REQUEST_TIMEOUT_MILLIS);

        final CompletableFuture<AggregatedHttpResponse> response = sendHttpRequest();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(callbacks, hasSize(1)));
        assertThat(response.isDone(), equalTo(false));

        callbacks.get(0).accept(true);

        assertThat(response.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).status(), equalTo(HttpStatus.OK));
        verify(buffer).writeAll(any(), anyInt());
    }

    @Test
    void http_request_with_negative_acknowledgement_returns_too_many_requests_with_retry_after() throws Exception {
        startObjectUnderTest(REQUEST_TIMEOUT_MILLIS);

        final CompletableFuture<AggregatedHttpResponse> response = sendHttpRequest();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(callbacks, hasSize(1)));

        callbacks.get(0).accept(false);

        final AggregatedHttpResponse aggregatedResponse = response.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(aggregatedResponse.status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
        assertThat(aggregatedResponse.headers().get(HttpHeaderNames.RETRY_AFTER), notNullValue());
    }

    @Test
    void stop_waits_for_pending_request_and_rejects_new_requests() throws Exception {
        startObjectUnderTest(REQUEST_TIMEOUT_MILLIS);
        final CompletableFuture<AggregatedHttpResponse> pendingResponse = sendHttpRequest();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(callbacks, hasSize(1)));

        final OTelTraceSource stopping = objectUnderTest;
        objectUnderTest = null;
        final Thread stopThread = new Thread(stopping::stop);
        stopThread.start();
        await().atMost(Duration.ofSeconds(5)).until(() -> stopThread.getState() == Thread.State.TIMED_WAITING);

        assertThat(sendHttpRequest().get(TIMEOUT_SECONDS, TimeUnit.SECONDS).status(), equalTo(HttpStatus.SERVICE_UNAVAILABLE));
        assertThat(stopThread.isAlive(), equalTo(true));

        callbacks.get(0).accept(true);

        assertThat(pendingResponse.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).status(), equalTo(HttpStatus.OK));
        await().atMost(Duration.ofSeconds(TIMEOUT_SECONDS)).until(() -> !stopThread.isAlive());
        assertThat(callbacks, hasSize(1));
    }

    private TraceServiceGrpc.TraceServiceBlockingStub createGrpcClient() {
        return Clients.builder(GRPC_ENDPOINT)
                .responseTimeoutMillis(30000)
                .build(TraceServiceGrpc.TraceServiceBlockingStub.class);
    }

    private CompletableFuture<AggregatedHttpResponse> sendHttpRequest() throws Exception {
        return WebClient.builder().responseTimeoutMillis(30000).build().execute(
                RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path(HTTP_PATH)
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(createRequest()).getBytes()))
                .aggregate();
    }

    private static ExportTraceServiceRequest createRequest() {
        return ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(ResourceSpans.newBuilder()
                        .addScopeSpans(ScopeSpans.newBuilder()
                                .addSpans(Span.newBuilder()
                                        .setTraceId(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                                        .setSpanId(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                                        .setName(UUID.randomUUID().toString())
                                        .setKind(Span.SpanKind.SPAN_KIND_SERVER)
                                        .setStartTimeUnixNano(100)
                                        .setEndTimeUnixNano(101)
                                        .build())
                                .build())
                        .build())
                .build();
    }
}
