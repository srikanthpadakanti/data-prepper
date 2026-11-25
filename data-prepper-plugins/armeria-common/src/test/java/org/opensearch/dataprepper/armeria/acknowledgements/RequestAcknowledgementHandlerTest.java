/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.armeria.acknowledgements;

import com.linecorp.armeria.common.logging.RequestLog;
import com.linecorp.armeria.common.logging.RequestLogAccess;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.exceptions.NegativeAcknowledgementException;
import org.opensearch.dataprepper.exceptions.SourceShuttingDownException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.ToDoubleFunction;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RequestAcknowledgementHandlerTest {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(10);

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;
    @Mock
    private AcknowledgementSet acknowledgementSet;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter requestsAcknowledgedCounter;
    @Mock
    private Counter requestsNegativelyAcknowledgedCounter;
    @Mock
    private Counter requestsRejectedShuttingDownCounter;
    @Mock
    private ServiceRequestContext requestContext;
    @Mock
    private RequestLogAccess requestLogAccess;
    @Mock
    private StreamObserver<String> responseObserver;
    @Mock
    private Runnable onSuccess;
    @Captor
    private ArgumentCaptor<Consumer<Boolean>> callbackCaptor;
    @Captor
    private ArgumentCaptor<AtomicInteger> pendingGaugeCaptor;
    @Captor
    private ArgumentCaptor<ToDoubleFunction<AtomicInteger>> pendingGaugeFunctionCaptor;

    private CompletableFuture<RequestLog> requestCompleted;

    @BeforeEach
    void setUp() {
        requestCompleted = new CompletableFuture<>();
    }

    private RequestAcknowledgementHandler createObjectUnderTest() {
        when(pluginMetrics.counter(RequestAcknowledgementHandler.REQUESTS_ACKNOWLEDGED)).thenReturn(requestsAcknowledgedCounter);
        when(pluginMetrics.counter(RequestAcknowledgementHandler.REQUESTS_NEGATIVELY_ACKNOWLEDGED))
                .thenReturn(requestsNegativelyAcknowledgedCounter);
        when(pluginMetrics.counter(RequestAcknowledgementHandler.REQUESTS_REJECTED_SHUTTING_DOWN))
                .thenReturn(requestsRejectedShuttingDownCounter);
        return new RequestAcknowledgementHandler(acknowledgementSetManager, REQUEST_TIMEOUT, pluginMetrics);
    }

    private void stubAcknowledgementSet() {
        when(acknowledgementSetManager.create(callbackCaptor.capture(), any(Duration.class), anyBoolean())).thenReturn(acknowledgementSet);
        when(requestContext.log()).thenReturn(requestLogAccess);
        when(requestLogAccess.whenComplete()).thenReturn(requestCompleted);
    }

    private double pendingRequestsGaugeValue() {
        verify(pluginMetrics).gauge(eq(RequestAcknowledgementHandler.REQUESTS_PENDING_ACKNOWLEDGEMENT),
                pendingGaugeCaptor.capture(), pendingGaugeFunctionCaptor.capture());
        return pendingGaugeFunctionCaptor.getValue().applyAsDouble(pendingGaugeCaptor.getValue());
    }

    private static Stream<Arguments> nullConstructorArguments() {
        return Stream.of(
                Arguments.of(null, REQUEST_TIMEOUT, mock(PluginMetrics.class)),
                Arguments.of(mock(AcknowledgementSetManager.class), null, mock(PluginMetrics.class)),
                Arguments.of(mock(AcknowledgementSetManager.class), REQUEST_TIMEOUT, null));
    }

    @ParameterizedTest
    @MethodSource("nullConstructorArguments")
    void constructor_with_null_argument_throws(final AcknowledgementSetManager manager, final Duration timeout,
                                               final PluginMetrics metrics) {
        assertThrows(NullPointerException.class, () -> new RequestAcknowledgementHandler(manager, timeout, metrics));
    }

    @Test
    void create_adds_every_event_and_uses_request_timeout_without_expiry_callback() {
        stubAcknowledgementSet();
        final Event event = mock(Event.class);
        final List<Record<Object>> records = List.of(new Record<>(event), new Record<>(UUID.randomUUID().toString()));

        createObjectUnderTest().create(requestContext, records);

        verify(acknowledgementSetManager).create(any(), eq(REQUEST_TIMEOUT), eq(false));
        verify(acknowledgementSet).add(event);
        verify(acknowledgementSet, never()).complete();
    }

    @Test
    void complete_completes_set_and_positive_acknowledgement_completes_future() {
        stubAcknowledgementSet();
        final PendingRequestAcknowledgement pending = createObjectUnderTest().create(requestContext, Collections.emptyList());

        final CompletableFuture<Void> acknowledged = pending.complete();

        verify(acknowledgementSet).complete();
        assertThat(acknowledged.isDone(), equalTo(false));
        callbackCaptor.getValue().accept(true);
        assertThat(acknowledged.isDone(), equalTo(true));
        assertThat(acknowledged.isCompletedExceptionally(), equalTo(false));
        verify(requestsAcknowledgedCounter).increment();
        verifyNoInteractions(requestsNegativelyAcknowledgedCounter);
    }

    @Test
    void negative_acknowledgement_completes_future_with_NegativeAcknowledgementException() {
        stubAcknowledgementSet();
        final CompletableFuture<Void> acknowledged = createObjectUnderTest().create(requestContext, Collections.emptyList()).complete();

        callbackCaptor.getValue().accept(false);

        final CompletionException exception = assertThrows(CompletionException.class, acknowledged::join);
        assertThat(exception.getCause(), instanceOf(NegativeAcknowledgementException.class));
        verify(requestsNegativelyAcknowledgedCounter).increment();
        verifyNoInteractions(requestsAcknowledgedCounter);
    }

    @Test
    void create_after_prepareForShutdown_throws_SourceShuttingDownException() {
        final RequestAcknowledgementHandler objectUnderTest = createObjectUnderTest();
        objectUnderTest.prepareForShutdown();

        assertThrows(SourceShuttingDownException.class, () -> objectUnderTest.create(requestContext, Collections.emptyList()));

        verify(acknowledgementSetManager, never()).create(any(), any(Duration.class), anyBoolean());
        verify(requestsRejectedShuttingDownCounter).increment();
        assertThat(pendingRequestsGaugeValue(), equalTo(0.0));
    }

    @Test
    void request_is_pending_until_its_response_is_complete() {
        stubAcknowledgementSet();
        final RequestAcknowledgementHandler objectUnderTest = createObjectUnderTest();

        objectUnderTest.create(requestContext, Collections.emptyList());
        assertThat(pendingRequestsGaugeValue(), equalTo(1.0));

        requestCompleted.complete(mock(RequestLog.class));
        assertThat(pendingRequestsGaugeValue(), equalTo(0.0));
    }

    @Test
    void awaitPendingRequests_returns_true_when_nothing_is_pending() {
        assertThat(createObjectUnderTest().awaitPendingRequests(Duration.ofSeconds(5)), equalTo(true));
    }

    @Test
    void awaitPendingRequests_returns_false_when_request_still_pending_at_timeout() {
        stubAcknowledgementSet();
        final RequestAcknowledgementHandler objectUnderTest = createObjectUnderTest();
        objectUnderTest.create(requestContext, Collections.emptyList());

        assertThat(objectUnderTest.awaitPendingRequests(Duration.ofMillis(50)), equalTo(false));
    }

    @Test
    void awaitPendingRequests_returns_true_once_pending_request_completes() throws Exception {
        stubAcknowledgementSet();
        final RequestAcknowledgementHandler objectUnderTest = createObjectUnderTest();
        objectUnderTest.create(requestContext, Collections.emptyList());
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            final Future<Boolean> awaited = executorService.submit(() -> objectUnderTest.awaitPendingRequests(Duration.ofSeconds(30)));

            requestCompleted.complete(mock(RequestLog.class));

            assertThat(awaited.get(10, TimeUnit.SECONDS), equalTo(true));
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void awaitPendingRequests_when_interrupted_returns_false_and_keeps_interrupt_status() {
        stubAcknowledgementSet();
        final RequestAcknowledgementHandler objectUnderTest = createObjectUnderTest();
        objectUnderTest.create(requestContext, Collections.emptyList());
        Thread.currentThread().interrupt();
        try {
            assertThat(objectUnderTest.awaitPendingRequests(Duration.ofSeconds(30)), equalTo(false));
            assertThat(Thread.currentThread().isInterrupted(), equalTo(true));
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void respondWhenAcknowledged_on_success_runs_onSuccess_and_sends_response() {
        final CompletableFuture<Void> acknowledged = new CompletableFuture<>();
        final String response = UUID.randomUUID().toString();
        createObjectUnderTest().respondWhenAcknowledged(acknowledged, requestContext, responseObserver, response, onSuccess);
        verifyNoInteractions(responseObserver);

        acknowledged.complete(null);

        final InOrder inOrder = inOrder(onSuccess, responseObserver);
        inOrder.verify(onSuccess).run();
        inOrder.verify(responseObserver).onNext(response);
        inOrder.verify(responseObserver).onCompleted();
        verify(responseObserver, never()).onError(any());
    }

    @Test
    void respondWhenAcknowledged_on_failure_sends_error() {
        final CompletableFuture<Void> acknowledged = new CompletableFuture<>();
        final NegativeAcknowledgementException failure = new NegativeAcknowledgementException(UUID.randomUUID().toString());
        createObjectUnderTest().respondWhenAcknowledged(acknowledged, requestContext, responseObserver,
                UUID.randomUUID().toString(), onSuccess);

        acknowledged.completeExceptionally(failure);

        verify(responseObserver).onError(failure);
        verify(responseObserver, never()).onNext(any());
        verifyNoInteractions(onSuccess);
    }

    @Test
    void respondWhenAcknowledged_after_timeout_sends_nothing() {
        when(requestContext.isTimedOut()).thenReturn(true);
        final CompletableFuture<Void> acknowledged = new CompletableFuture<>();
        createObjectUnderTest().respondWhenAcknowledged(acknowledged, requestContext, responseObserver,
                UUID.randomUUID().toString(), onSuccess);

        acknowledged.complete(null);

        verifyNoInteractions(responseObserver);
        verifyNoInteractions(onSuccess);
    }
}
