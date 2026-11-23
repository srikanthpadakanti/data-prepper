/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.otellogs.http;


import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.opensearch.dataprepper.InvalidRequestExceptions;
import org.opensearch.dataprepper.RetryInfoCalculator;
import org.opensearch.dataprepper.exceptions.BadRequestException;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.exceptions.RequestCancelledException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.rpc.RetryInfo;
import com.linecorp.armeria.common.ContentTooLargeException;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.ResponseHeaders;
import com.linecorp.armeria.common.ResponseHeadersBuilder;
import com.linecorp.armeria.server.HttpStatusException;
import com.linecorp.armeria.server.RequestTimeoutException;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ExceptionHandlerFunction;

import io.grpc.Status;
import io.micrometer.core.instrument.Counter;

public class HttpExceptionHandler implements ExceptionHandlerFunction {
    private static final Logger LOG = LoggerFactory.getLogger(HttpExceptionHandler.class);

    static final String ARMERIA_REQUEST_TIMEOUT_MESSAGE = "Timeout waiting for request to be served. This is usually due to the buffer being full.";
    public static final String REQUEST_TIMEOUTS = "requestTimeouts";
    public static final String BAD_REQUESTS = "badRequests";
    public static final String REQUESTS_TOO_LARGE = "requestsTooLarge";
    public static final String INTERNAL_SERVER_ERROR = "internalServerError";
    private static final long MINIMUM_RETRY_AFTER_SECONDS = 1;

    private final Counter requestTimeoutsCounter;
    private final Counter badRequestsCounter;
    private final Counter requestsTooLargeCounter;
    private final Counter internalServerErrorCounter;
    private final RetryInfoCalculator retryInfoCalculator;

    public HttpExceptionHandler(final PluginMetrics pluginMetrics, Duration retryInfoMinDelay, Duration retryInfoMaxDelay) {
        requestTimeoutsCounter = pluginMetrics.counter(REQUEST_TIMEOUTS);
        badRequestsCounter = pluginMetrics.counter(BAD_REQUESTS);
        requestsTooLargeCounter = pluginMetrics.counter(REQUESTS_TOO_LARGE);
        internalServerErrorCounter = pluginMetrics.counter(INTERNAL_SERVER_ERROR);
        this.retryInfoCalculator = new RetryInfoCalculator(retryInfoMinDelay, retryInfoMaxDelay);
    }

    @Override
    public HttpResponse handleException(final ServiceRequestContext ctx,
                                        final HttpRequest req,
                                        final Throwable e) {
        Throwable exceptionCause = e;
        while ((exceptionCause instanceof BufferWriteException || exceptionCause instanceof HttpStatusException)
                && exceptionCause.getCause() != null) {
            exceptionCause = exceptionCause.getCause();
        }

        StatusHolder statusHolder = createStatus(exceptionCause);

        try {
            JsonFormat.TypeRegistry typeRegistry = JsonFormat.TypeRegistry.newBuilder()
                    .add(RetryInfo.getDescriptor())
                    .build();

            JsonFormat.Printer printer = JsonFormat.printer().usingTypeRegistry(typeRegistry);
            final ResponseHeadersBuilder headersBuilder = ResponseHeaders.builder(statusHolder.getHttpStatus())
                    .contentType(MediaType.JSON);
            if (statusHolder.getHttpStatus() == HttpStatus.TOO_MANY_REQUESTS) {
                headersBuilder.add(HttpHeaderNames.RETRY_AFTER, createRetryAfter(statusHolder.getStatus().getDetailsList()));
            }
            return HttpResponse.of(headersBuilder.build(), HttpData.ofUtf8(printer.print(statusHolder.getStatus())));
        } catch (InvalidProtocolBufferException ipbe) {
            throw new RuntimeException(ipbe);
        }
    }

    private StatusHolder createStatus(Throwable e) {
        if (e instanceof RequestTimeoutException || e instanceof TimeoutException) {
            requestTimeoutsCounter.increment();
            return new StatusHolder(createStatus(e, Status.Code.RESOURCE_EXHAUSTED).toBuilder()
                    .addDetails(Any.pack(retryInfoCalculator.createRetryInfo()))
                    .build(), HttpStatus.TOO_MANY_REQUESTS);
        } else if (e instanceof SizeOverflowException || e instanceof ContentTooLargeException) {
            requestsTooLargeCounter.increment();
            return new StatusHolder(createStatus(e, Status.Code.RESOURCE_EXHAUSTED), HttpStatus.REQUEST_ENTITY_TOO_LARGE);
        } else if (e instanceof BadRequestException) {
            badRequestsCounter.increment();
            return new StatusHolder(createStatus(e, Status.Code.INVALID_ARGUMENT), createHttpStatusFromProtoBufStatus(Status.Code.INVALID_ARGUMENT));
        } else if (InvalidRequestExceptions.isInvalidProtobuf(e)) {
            badRequestsCounter.increment();
            return new StatusHolder(createStatus(Status.Code.INVALID_ARGUMENT, InvalidRequestExceptions.INVALID_PROTOBUF_MESSAGE),
                    createHttpStatusFromProtoBufStatus(Status.Code.INVALID_ARGUMENT));
        } else if (InvalidRequestExceptions.isUndecodableCompressedFrame(e)) {
            badRequestsCounter.increment();
            return new StatusHolder(createStatus(Status.Code.INVALID_ARGUMENT, InvalidRequestExceptions.COMPRESSED_FRAME_MESSAGE),
                    createHttpStatusFromProtoBufStatus(Status.Code.INVALID_ARGUMENT));
        } else if (e instanceof RequestCancelledException) {
            requestTimeoutsCounter.increment();
            return new StatusHolder(createStatus(e, Status.Code.CANCELLED), createHttpStatusFromProtoBufStatus(Status.Code.CANCELLED));
        } else {
            LOG.error("Unexpected exception handling http request", e);
            internalServerErrorCounter.increment();
            return new StatusHolder(createStatus(e, Status.Code.INTERNAL), createHttpStatusFromProtoBufStatus(Status.Code.INTERNAL));
        }
    }

    private HttpStatus createHttpStatusFromProtoBufStatus(Status.Code status) {
        if (status == Status.Code.INVALID_ARGUMENT) {
            return HttpStatus.BAD_REQUEST;
        } else {
            return HttpStatus.INTERNAL_SERVER_ERROR;
        }
    }

    private String createRetryAfter(final List<Any> details) throws InvalidProtocolBufferException {
        for (final Any detail : details) {
            if (detail.is(RetryInfo.class)) {
                final RetryInfo retryInfo = detail.unpack(RetryInfo.class);
                final long roundedUpSeconds = retryInfo.getRetryDelay().getSeconds() + (retryInfo.getRetryDelay().getNanos() > 0 ? 1 : 0);
                return Long.toString(Math.max(MINIMUM_RETRY_AFTER_SECONDS, roundedUpSeconds));
            }
        }
        return Long.toString(MINIMUM_RETRY_AFTER_SECONDS);
    }

    private com.google.rpc.Status createStatus(final Throwable e, final Status.Code code) {
        final String message = e instanceof RequestTimeoutException
                ? ARMERIA_REQUEST_TIMEOUT_MESSAGE
                : (e.getMessage() == null ? code.name() : e.getMessage());
        return createStatus(code, message);
    }

    private com.google.rpc.Status createStatus(final Status.Code code, final String message) {
        com.google.rpc.Status.Builder builder = com.google.rpc.Status.newBuilder()
                .setCode(code.value())
                .setMessage(message);
        return builder.build();
    }

    private static class StatusHolder {
        private final HttpStatus httpStatus;
        private final com.google.rpc.Status status;

        public StatusHolder(com.google.rpc.Status status, HttpStatus httpStatus) {
            this.httpStatus = httpStatus;
            this.status = status;
        }

        public HttpStatus getHttpStatus() {
            return httpStatus;
        }

        public com.google.rpc.Status getStatus() {
            return status;
        }
    }

}
