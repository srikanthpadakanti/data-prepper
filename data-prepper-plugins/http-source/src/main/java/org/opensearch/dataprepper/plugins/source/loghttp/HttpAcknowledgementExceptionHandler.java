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

import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.ResponseHeaders;
import com.linecorp.armeria.server.RequestTimeoutException;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;
import io.micrometer.core.instrument.Counter;
import io.netty.util.AttributeKey;
import org.opensearch.dataprepper.HttpRequestExceptionHandler;
import org.opensearch.dataprepper.exceptions.NegativeAcknowledgementException;
import org.opensearch.dataprepper.exceptions.SourceShuttingDownException;
import org.opensearch.dataprepper.metrics.PluginMetrics;

import java.util.Objects;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

class HttpAcknowledgementExceptionHandler implements ExceptionHandlerFunction {
    static final String ACKNOWLEDGMENT_TIMEOUTS = "acknowledgmentTimeouts";
    static final AttributeKey<Boolean> REQUEST_ACCEPTED = AttributeKey.valueOf(HttpAcknowledgementExceptionHandler.class, "requestAccepted");
    private static final String ACKNOWLEDGMENT_TIMEOUT_MESSAGE = "The events in the request were not acknowledged in time. Retry the request.";
    private static final String BUFFER_FULL_MESSAGE = "The buffer is full. Retry the request.";

    private final HttpRequestExceptionHandler delegate;
    private final RetryAfterCalculator retryAfterCalculator;
    private final Counter acknowledgmentTimeoutsCounter;

    HttpAcknowledgementExceptionHandler(final HttpRequestExceptionHandler delegate,
                                        final RetryAfterCalculator retryAfterCalculator,
                                        final PluginMetrics pluginMetrics) {
        this.delegate = Objects.requireNonNull(delegate);
        this.retryAfterCalculator = Objects.requireNonNull(retryAfterCalculator);
        this.acknowledgmentTimeoutsCounter = Objects.requireNonNull(pluginMetrics).counter(ACKNOWLEDGMENT_TIMEOUTS);
    }

    @Override
    public HttpResponse handleException(final ServiceRequestContext ctx, final HttpRequest req, final Throwable cause) {
        final Throwable exception = unwrap(cause);
        if (exception instanceof NegativeAcknowledgementException) {
            return retryableResponse(HttpStatus.TOO_MANY_REQUESTS, exception.getMessage());
        }
        if (exception instanceof SourceShuttingDownException) {
            return retryableResponse(HttpStatus.SERVICE_UNAVAILABLE, exception.getMessage());
        }
        if (exception instanceof TimeoutException) {
            return retryableResponse(HttpStatus.TOO_MANY_REQUESTS, BUFFER_FULL_MESSAGE);
        }
        if (exception instanceof RequestTimeoutException && Boolean.TRUE.equals(ctx.attr(REQUEST_ACCEPTED))) {
            acknowledgmentTimeoutsCounter.increment();
            return retryableResponse(HttpStatus.TOO_MANY_REQUESTS, ACKNOWLEDGMENT_TIMEOUT_MESSAGE);
        }
        return delegate.handleException(ctx, req, exception);
    }

    HttpResponse retryableResponse(final HttpStatus status, final String message) {
        return HttpResponse.of(ResponseHeaders.builder(status)
                        .contentType(MediaType.ANY_TYPE)
                        .add(HttpHeaderNames.RETRY_AFTER, retryAfterCalculator.nextRetryAfter())
                        .build(),
                HttpData.ofUtf8(message == null ? status.reasonPhrase() : message));
    }

    private static Throwable unwrap(final Throwable cause) {
        Throwable exception = cause;
        while ((exception instanceof CompletionException || exception instanceof ExecutionException) && exception.getCause() != null) {
            exception = exception.getCause();
        }
        return exception;
    }
}
