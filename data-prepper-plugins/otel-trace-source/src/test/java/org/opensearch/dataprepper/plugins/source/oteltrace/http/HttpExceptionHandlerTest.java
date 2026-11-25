/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.oteltrace.http;


import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.HttpRequestExceptionHandler;
import org.opensearch.dataprepper.exceptions.BadRequestException;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.exceptions.NegativeAcknowledgementException;
import org.opensearch.dataprepper.exceptions.RequestCancelledException;
import org.opensearch.dataprepper.exceptions.SourceShuttingDownException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.ContentTooLargeException;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.HttpStatusException;
import com.linecorp.armeria.server.RequestTimeoutException;
import com.linecorp.armeria.server.ServiceRequestContext;

import io.grpc.Status;
import io.micrometer.core.instrument.Counter;

@ExtendWith(MockitoExtension.class)
class HttpExceptionHandlerTest {
    HttpExceptionHandler httpExceptionHandler;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private ServiceRequestContext requestContext;

    @Mock
    private HttpRequest httpRequest;

    @Mock
    private Counter requestTimeoutsCounter;

    @Mock
    private Counter badRequestsCounter;

    @Mock
    private Counter requestsTooLargeCounter;

    @Mock
    private Counter internalServerErrorCounter;
    @BeforeEach
    public void setUp() {
        when(pluginMetrics.counter(HttpRequestExceptionHandler.REQUEST_TIMEOUTS)).thenReturn(requestTimeoutsCounter);
        when(pluginMetrics.counter(HttpRequestExceptionHandler.BAD_REQUESTS)).thenReturn(badRequestsCounter);
        when(pluginMetrics.counter(HttpRequestExceptionHandler.REQUESTS_TOO_LARGE)).thenReturn(requestsTooLargeCounter);
        when(pluginMetrics.counter(HttpRequestExceptionHandler.INTERNAL_SERVER_ERROR)).thenReturn(internalServerErrorCounter);
        httpExceptionHandler = new HttpExceptionHandler(pluginMetrics, Duration.ofMillis(100), Duration.ofSeconds(2));
    }

    @Test
    public void testHandleBadRequestException() {
        httpExceptionHandler.handleException(requestContext, httpRequest, new BadRequestException("msg", null));
        verify(badRequestsCounter).increment();
    }

    @Test
    public void testHandleTimeoutException() {
        httpExceptionHandler.handleException(requestContext, httpRequest, new BufferWriteException(null, new TimeoutException()));
        verify(requestTimeoutsCounter, times(1)).increment();
    }

    @Test
    public void testHandleArmeriaTimeoutException() {
        httpExceptionHandler.handleException(requestContext, httpRequest, RequestTimeoutException.get());
        verify(requestTimeoutsCounter, times(1)).increment();
    }

    @Test
    public void testHandleSizeOverflowException() {
        httpExceptionHandler.handleException(requestContext, httpRequest, new SizeOverflowException("msg"));
        verify(requestsTooLargeCounter).increment();
    }

    @Test
    public void testHandleRequestCancelledException() {
        httpExceptionHandler.handleException(requestContext, httpRequest, new RequestCancelledException("msg"));
        verify(requestTimeoutsCounter, times(1)).increment();
    }

    @Test
    public void testHandleInternalServerException() {
        httpExceptionHandler.handleException(requestContext, httpRequest, new RuntimeException("msg"));
        verify(internalServerErrorCounter, times(1)).increment();
    }

    @ParameterizedTest
    @MethodSource("timeoutExceptions")
    public void handleException_with_timeout_returns_too_many_requests(final Throwable throwable) {
        assertThat(handle(httpExceptionHandler, throwable).status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
    }

    @ParameterizedTest
    @MethodSource("timeoutExceptions")
    public void handleException_with_timeout_returns_retry_after_of_minimum_delay(final Throwable throwable) {
        assertThat(handle(httpExceptionHandler, throwable).headers().get(HttpHeaderNames.RETRY_AFTER), equalTo("1"));
    }

    @ParameterizedTest
    @MethodSource("timeoutExceptions")
    public void handleException_with_timeout_returns_body_with_retry_info(final Throwable throwable) {
        assertThat(handle(httpExceptionHandler, throwable).contentUtf8(), containsString("retryDelay"));
    }

    @Test
    public void handleException_with_repeated_timeouts_returns_retry_after_growing_to_max_delay() {
        final HttpExceptionHandler handler = new HttpExceptionHandler(pluginMetrics, Duration.ofSeconds(10), Duration.ofSeconds(50));
        assertThat(retryAfterForTimeout(handler), equalTo("10"));
        assertThat(retryAfterForTimeout(handler), equalTo("10"));
        assertThat(retryAfterForTimeout(handler), equalTo("20"));
        assertThat(retryAfterForTimeout(handler), equalTo("40"));
        assertThat(retryAfterForTimeout(handler), equalTo("50"));
        assertThat(retryAfterForTimeout(handler), equalTo("50"));
    }

    @Test
    public void handleException_with_fractional_retry_delay_rounds_retry_after_up() {
        final HttpExceptionHandler handler = new HttpExceptionHandler(pluginMetrics, Duration.ofMillis(1500), Duration.ofSeconds(5));
        assertThat(retryAfterForTimeout(handler), equalTo("2"));
    }

    @Test
    public void handleException_with_zero_retry_delay_returns_minimum_retry_after() {
        final HttpExceptionHandler handler = new HttpExceptionHandler(pluginMetrics, Duration.ZERO, Duration.ZERO);
        assertThat(retryAfterForTimeout(handler), equalTo("1"));
    }

    @ParameterizedTest
    @MethodSource("tooLargeExceptions")
    public void handleException_with_too_large_request_returns_request_entity_too_large(final Throwable throwable) {
        assertThat(handle(httpExceptionHandler, throwable).status(), equalTo(HttpStatus.REQUEST_ENTITY_TOO_LARGE));
    }

    @ParameterizedTest
    @MethodSource("tooLargeExceptions")
    public void handleException_with_too_large_request_returns_no_retry_after(final Throwable throwable) {
        assertThat(handle(httpExceptionHandler, throwable).headers().get(HttpHeaderNames.RETRY_AFTER), nullValue());
    }

    @ParameterizedTest
    @MethodSource("tooLargeExceptions")
    public void handleException_with_too_large_request_returns_body_without_retry_info(final Throwable throwable) {
        assertThat(handle(httpExceptionHandler, throwable).contentUtf8(), not(containsString("retryDelay")));
    }

    @ParameterizedTest
    @MethodSource("tooLargeExceptions")
    public void handleException_with_too_large_request_increments_requests_too_large_counter(final Throwable throwable) {
        handle(httpExceptionHandler, throwable);
        verify(requestsTooLargeCounter).increment();
    }

    @ParameterizedTest
    @MethodSource("tooLargeExceptions")
    public void handleException_with_too_large_request_does_not_advance_retry_after_backoff(final Throwable throwable) {
        final HttpExceptionHandler handler = new HttpExceptionHandler(pluginMetrics, Duration.ofSeconds(10), Duration.ofSeconds(50));
        handle(handler, throwable);
        handle(handler, throwable);
        handle(handler, throwable);
        assertThat(retryAfterForTimeout(handler), equalTo("10"));
    }

    @ParameterizedTest
    @MethodSource("exceptionsWithoutRetryAfter")
    public void handleException_with_non_retryable_exception_returns_expected_status(final Throwable throwable,
            final HttpStatus expectedStatus) {
        assertThat(handle(httpExceptionHandler, throwable).status(), equalTo(expectedStatus));
    }

    @ParameterizedTest
    @MethodSource("exceptionsWithoutRetryAfter")
    public void handleException_with_non_retryable_exception_returns_no_retry_after(final Throwable throwable,
            final HttpStatus expectedStatus) {
        assertThat(handle(httpExceptionHandler, throwable).headers().get(HttpHeaderNames.RETRY_AFTER), nullValue());
    }

    @Test
    public void handleException_with_http_status_exception_without_cause_increments_internal_server_error_counter() {
        handle(httpExceptionHandler, HttpStatusException.of(HttpStatus.SERVICE_UNAVAILABLE));
        verify(internalServerErrorCounter).increment();
    }

    @ParameterizedTest
    @MethodSource("acknowledgementExceptions")
    public void handleException_with_acknowledgement_failure_returns_expected_status(final Throwable throwable,
                                                                                    final HttpStatus expectedStatus) {
        assertThat(handle(httpExceptionHandler, throwable).status(), equalTo(expectedStatus));
    }

    @ParameterizedTest
    @MethodSource("acknowledgementExceptions")
    public void handleException_with_acknowledgement_failure_returns_retry_after(final Throwable throwable,
                                                                                final HttpStatus expectedStatus) {
        assertThat(handle(httpExceptionHandler, throwable).headers().get(HttpHeaderNames.RETRY_AFTER), equalTo("1"));
    }

    @ParameterizedTest
    @MethodSource("acknowledgementExceptions")
    public void handleException_with_acknowledgement_failure_returns_unavailable_body_with_retry_info(final Throwable throwable,
                                                                                                     final HttpStatus expectedStatus) {
        final String body = handle(httpExceptionHandler, throwable).contentUtf8();
        assertThat(body, containsString("retryDelay"));
        assertThat(body, containsString(throwable.getMessage()));
        assertThat(body, containsString("\"code\": " + Status.Code.UNAVAILABLE.value()));
    }

    @ParameterizedTest
    @MethodSource("acknowledgementExceptions")
    public void handleException_with_acknowledgement_failure_does_not_count_internal_server_error(final Throwable throwable,
                                                                                                 final HttpStatus expectedStatus) {
        handle(httpExceptionHandler, throwable);
        verifyNoInteractions(internalServerErrorCounter);
    }

    private static Stream<Arguments> acknowledgementExceptions() {
        return Stream.of(
                Arguments.of(new NegativeAcknowledgementException(UUID.randomUUID().toString()), HttpStatus.TOO_MANY_REQUESTS),
                Arguments.of(new SourceShuttingDownException(UUID.randomUUID().toString()), HttpStatus.SERVICE_UNAVAILABLE));
    }

    private static Stream<Throwable> timeoutExceptions() {
        return Stream.of(
                new BufferWriteException(UUID.randomUUID().toString(), new TimeoutException()),
                new TimeoutException(UUID.randomUUID().toString()),
                HttpStatusException.of(HttpStatus.SERVICE_UNAVAILABLE,
                        new BufferWriteException(UUID.randomUUID().toString(), new TimeoutException())),
                RequestTimeoutException.get());
    }

    private static Stream<Throwable> tooLargeExceptions() {
        return Stream.of(
                new SizeOverflowException(UUID.randomUUID().toString()),
                new BufferWriteException(UUID.randomUUID().toString(), new SizeOverflowException(UUID.randomUUID().toString())),
                ContentTooLargeException.get(),
                HttpStatusException.of(HttpStatus.BAD_REQUEST, new SizeOverflowException(UUID.randomUUID().toString())),
                HttpStatusException.of(HttpStatus.REQUEST_ENTITY_TOO_LARGE, ContentTooLargeException.get()));
    }

    private static Stream<Arguments> exceptionsWithoutRetryAfter() {
        return Stream.of(
                arguments(new BadRequestException(UUID.randomUUID().toString(), null), HttpStatus.BAD_REQUEST),
                arguments(new RequestCancelledException(UUID.randomUUID().toString()), HttpStatus.INTERNAL_SERVER_ERROR),
                arguments(new RuntimeException(UUID.randomUUID().toString()), HttpStatus.INTERNAL_SERVER_ERROR),
                arguments(new BufferWriteException(UUID.randomUUID().toString(), null), HttpStatus.INTERNAL_SERVER_ERROR),
                arguments(HttpStatusException.of(HttpStatus.SERVICE_UNAVAILABLE), HttpStatus.INTERNAL_SERVER_ERROR));
    }

    private String retryAfterForTimeout(final HttpExceptionHandler handler) {
        return handle(handler, RequestTimeoutException.get()).headers().get(HttpHeaderNames.RETRY_AFTER);
    }

    private AggregatedHttpResponse handle(final HttpExceptionHandler handler, final Throwable throwable) {
        return handler.handleException(requestContext, httpRequest, throwable).aggregate().join();
    }
}
