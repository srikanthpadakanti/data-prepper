/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.otellogs;

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
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigFixture.createDefaultConfigBuilder;

@ExtendWith(MockitoExtension.class)
class OTelLogsSourceAcknowledgementsTest {
    private static final String GRPC_ENDPOINT = "gproto+http://127.0.0.1:21892/";
    private static final String HTTP_PATH = "/v1/logs";
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
    private OTelLogsSource objectUnderTest;

    @BeforeEach
    void setUp() {
        when(pipelineDescription.getPipelineName()).thenReturn(UUID.randomUUID().toString());
    }

    @AfterEach
    void tearDown() {
        if (objectUnderTest != null) {
            callbacks.forEach(callback -> callback.accept(true));
            objectUnderTest.stop();
        }
    }

    private OTelLogsSource createObjectUnderTest(final int requestTimeoutMillis) {
        final OTelLogsSourceConfig config = createDefaultConfigBuilder()
                .httpPath(HTTP_PATH)
                .acknowledgments(true)
                .requestTimeoutInMillis(requestTimeoutMillis)
                .build();
        return new OTelLogsSource(config, PluginMetrics.fromNames("otel_logs", "pipeline"), pluginFactory, pipelineDescription,
                acknowledgementSetManager);
    }

    private void startObjectUnderTest(final int requestTimeoutMillis) {
        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(authenticationProvider);
        when(acknowledgementSetManager.create(any(), any(Duration.class), anyBoolean())).thenAnswer(invocation -> {
            callbacks.add(invocation.getArgument(0));
            return mock(AcknowledgementSet.class);
        });
        objectUnderTest = createObjectUnderTest(requestTimeoutMillis);
        objectUnderTest.start(buffer);
    }

    @Test
    void acknowledgement_flags_follow_the_configuration() {
        final OTelLogsSource withAcknowledgements = createObjectUnderTest(REQUEST_TIMEOUT_MILLIS);
        assertThat(withAcknowledgements.areAcknowledgementsEnabled(), equalTo(true));
        assertThat(withAcknowledgements.requiresStrictAcknowledgementValidation(), equalTo(true));

        final OTelLogsSource withoutAcknowledgements = new OTelLogsSource(createDefaultConfigBuilder().build(),
                PluginMetrics.fromNames("otel_logs", "pipeline"), pluginFactory, pipelineDescription);
        assertThat(withoutAcknowledgements.areAcknowledgementsEnabled(), equalTo(false));
        assertThat(withoutAcknowledgements.requiresStrictAcknowledgementValidation(), equalTo(false));
    }

    @Test
    void grpc_request_is_answered_only_after_positive_acknowledgement() throws Exception {
        startObjectUnderTest(REQUEST_TIMEOUT_MILLIS);

        final CompletableFuture<ExportLogsServiceResponse> response = CompletableFuture.supplyAsync(() -> createGrpcClient().export(createRequest()));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(callbacks, hasSize(1)));
        assertThat(response.isDone(), equalTo(false));

        callbacks.get(0).accept(true);

        assertThat(response.get(TIMEOUT_SECONDS, TimeUnit.SECONDS), notNullValue());
    }

    @Test
    void grpc_request_with_negative_acknowledgement_returns_unavailable() throws Exception {
        startObjectUnderTest(REQUEST_TIMEOUT_MILLIS);

        final CompletableFuture<ExportLogsServiceResponse> response = CompletableFuture.supplyAsync(() -> createGrpcClient().export(createRequest()));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(callbacks, hasSize(1)));

        callbacks.get(0).accept(false);

        final ExecutionException exception = assertThrows(ExecutionException.class, () -> response.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertThat(exception.getCause(), instanceOf(StatusRuntimeException.class));
        assertThat(((StatusRuntimeException) exception.getCause()).getStatus().getCode(), equalTo(Status.Code.UNAVAILABLE));
    }

    @Test
    void grpc_request_without_acknowledgement_before_request_timeout_returns_resource_exhausted() {
        startObjectUnderTest(500);

        final StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> createGrpcClient().export(createRequest()));

        assertThat(exception.getStatus().getCode(), equalTo(Status.Code.RESOURCE_EXHAUSTED));
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
    void http_request_without_acknowledgement_before_request_timeout_returns_too_many_requests() throws Exception {
        startObjectUnderTest(500);

        assertThat(sendHttpRequest().get(TIMEOUT_SECONDS, TimeUnit.SECONDS).status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
    }

    @Test
    void stop_waits_for_pending_request_and_rejects_new_requests() throws Exception {
        startObjectUnderTest(REQUEST_TIMEOUT_MILLIS);
        final CompletableFuture<AggregatedHttpResponse> pendingResponse = sendHttpRequest();
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(callbacks, hasSize(1)));

        final OTelLogsSource stopping = objectUnderTest;
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

    private LogsServiceGrpc.LogsServiceBlockingStub createGrpcClient() {
        return Clients.builder(GRPC_ENDPOINT)
                .responseTimeoutMillis(30000)
                .build(LogsServiceGrpc.LogsServiceBlockingStub.class);
    }

    private CompletableFuture<AggregatedHttpResponse> sendHttpRequest() throws Exception {
        return WebClient.builder().responseTimeoutMillis(30000).build().execute(
                RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21892")
                        .method(HttpMethod.POST)
                        .path(HTTP_PATH)
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(createRequest()).getBytes()))
                .aggregate();
    }

    private static ExportLogsServiceRequest createRequest() {
        return ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(ResourceLogs.newBuilder()
                        .addScopeLogs(ScopeLogs.newBuilder()
                                .addLogRecords(LogRecord.newBuilder()
                                        .setBody(AnyValue.newBuilder().setStringValue(UUID.randomUUID().toString()).build())
                                        .build())
                                .build())
                        .build())
                .build();
    }
}
