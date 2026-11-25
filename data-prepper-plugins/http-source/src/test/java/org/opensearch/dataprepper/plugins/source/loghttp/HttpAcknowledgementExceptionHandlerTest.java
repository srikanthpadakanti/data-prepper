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

import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.RequestTimeoutException;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.HttpRequestExceptionHandler;
import org.opensearch.dataprepper.exceptions.NegativeAcknowledgementException;
import org.opensearch.dataprepper.exceptions.SourceShuttingDownException;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.UUID;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HttpAcknowledgementExceptionHandlerTest {
    private static final String RETRY_AFTER = "2";

    @Mock
    private HttpRequestExceptionHandler delegate;
    @Mock
    private RetryAfterCalculator retryAfterCalculator;
    @Mock
    private PluginMetrics pluginMetrics;
    @Mock
    private Counter acknowledgmentTimeoutsCounter;
    @Mock
    private ServiceRequestContext serviceRequestContext;
    @Mock
    private HttpRequest httpRequest;

    @BeforeEach
    void setUp() {
        when(pluginMetrics.counter(HttpAcknowledgementExceptionHandler.ACKNOWLEDGMENT_TIMEOUTS)).thenReturn(acknowledgmentTimeoutsCounter);
    }

    private HttpAcknowledgementExceptionHandler createObjectUnderTest() {
        return new HttpAcknowledgementExceptionHandler(delegate, retryAfterCalculator, pluginMetrics);
    }

    @Test
    void handleException_negative_acknowledgement_is_too_many_requests_with_retry_after() {
        when(retryAfterCalculator.nextRetryAfter()).thenReturn(RETRY_AFTER);

        final AggregatedHttpResponse response = createObjectUnderTest().handleException(serviceRequestContext, httpRequest,
                new CompletionException(new NegativeAcknowledgementException(UUID.randomUUID().toString()))).aggregate().join();

        assertThat(response.status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
        assertThat(response.headers().get(HttpHeaderNames.RETRY_AFTER), equalTo(RETRY_AFTER));
        verifyNoInteractions(delegate);
    }

    @Test
    void handleException_shutting_down_is_service_unavailable_with_retry_after() {
        when(retryAfterCalculator.nextRetryAfter()).thenReturn(RETRY_AFTER);

        final AggregatedHttpResponse response = createObjectUnderTest().handleException(serviceRequestContext, httpRequest,
                new SourceShuttingDownException(UUID.randomUUID().toString())).aggregate().join();

        assertThat(response.status(), equalTo(HttpStatus.SERVICE_UNAVAILABLE));
        assertThat(response.headers().get(HttpHeaderNames.RETRY_AFTER), equalTo(RETRY_AFTER));
    }

    @Test
    void handleException_buffer_timeout_is_too_many_requests_with_retry_after() {
        when(retryAfterCalculator.nextRetryAfter()).thenReturn(RETRY_AFTER);

        final AggregatedHttpResponse response = createObjectUnderTest().handleException(serviceRequestContext, httpRequest,
                new TimeoutException()).aggregate().join();

        assertThat(response.status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
        assertThat(response.headers().get(HttpHeaderNames.RETRY_AFTER), equalTo(RETRY_AFTER));
    }

    @Test
    void handleException_request_timeout_after_the_request_was_accepted_is_too_many_requests() {
        when(retryAfterCalculator.nextRetryAfter()).thenReturn(RETRY_AFTER);
        when(serviceRequestContext.attr(HttpAcknowledgementExceptionHandler.REQUEST_ACCEPTED)).thenReturn(Boolean.TRUE);

        final AggregatedHttpResponse response = createObjectUnderTest().handleException(serviceRequestContext, httpRequest,
                RequestTimeoutException.get()).aggregate().join();

        assertThat(response.status(), equalTo(HttpStatus.TOO_MANY_REQUESTS));
        assertThat(response.headers().get(HttpHeaderNames.RETRY_AFTER), equalTo(RETRY_AFTER));
        verify(acknowledgmentTimeoutsCounter).increment();
    }

    @Test
    void handleException_request_timeout_before_the_request_was_accepted_is_delegated() {
        final HttpResponse delegated = HttpResponse.of(HttpStatus.REQUEST_TIMEOUT);
        final RequestTimeoutException exception = RequestTimeoutException.get();
        when(delegate.handleException(serviceRequestContext, httpRequest, exception)).thenReturn(delegated);

        final HttpResponse response = createObjectUnderTest().handleException(serviceRequestContext, httpRequest, exception);

        assertThat(response, sameInstance(delegated));
        verify(acknowledgmentTimeoutsCounter, never()).increment();
    }

    @Test
    void handleException_other_exception_is_delegated_unwrapped() {
        final HttpResponse delegated = HttpResponse.of(HttpStatus.BAD_REQUEST);
        final IllegalArgumentException exception = new IllegalArgumentException(UUID.randomUUID().toString());
        when(delegate.handleException(serviceRequestContext, httpRequest, exception)).thenReturn(delegated);

        final HttpResponse response = createObjectUnderTest().handleException(serviceRequestContext, httpRequest,
                new CompletionException(exception));

        assertThat(response, sameInstance(delegated));
    }
}
