/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Post;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.armeria.acknowledgements.PendingRequestAcknowledgement;
import org.opensearch.dataprepper.armeria.acknowledgements.RequestAcknowledgementHandler;
import org.opensearch.dataprepper.http.codec.JsonCodec;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.codec.InputCodec;
import org.opensearch.dataprepper.model.log.JacksonLog;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.UUID;
import java.util.stream.Collectors;

/*
* A HTTP service for log ingestion to be executed by BlockingTaskExecutor.
*/

@Blocking
public class LogHTTPService {
    private static final int SERIALIZATION_OVERHEAD = 1024;
    public static final String REQUESTS_RECEIVED = "requestsReceived";
    public static final String SUCCESS_REQUESTS = "successRequests";
    public static final String REQUESTS_OVER_OPTIMAL_SIZE = "requestsOverOptimalSize";
    public static final String REQUESTS_OVER_MAXIMUM_SIZE = "requestsOverMaximumSize";
    public static final String PAYLOAD_SIZE = "payloadSize";
    public static final String REQUEST_PROCESS_DURATION = "requestProcessDuration";
    public static final String REQUESTS_REJECTED_PENDING_ACKNOWLEDGMENTS = "requestsRejectedPendingAcknowledgments";
    static final int ACKNOWLEDGEMENT_SET_EXPIRY_FACTOR = 2;
    private static final String PENDING_ACKNOWLEDGMENTS_MESSAGE =
            "The number of requests waiting for acknowledgments reaches max_pending_requests. Retry the request.";

    private static final Logger LOG = LoggerFactory.getLogger(LogHTTPService.class);

    // TODO: support other data-types as request body, e.g. json_lines, msgpack
    private final JsonCodec jsonCodec = new JsonCodec();
    private final Buffer<Record<Log>> buffer;
    private final InputCodec codec;
    private final int bufferWriteTimeoutInMillis;
    private final Counter requestsReceivedCounter;
    private final Counter successRequestsCounter;
    private final Counter requestsOverOptimalSizeCounter;
    private final Counter requestsOverMaximumSizeCounter;
    private final DistributionSummary payloadSizeSummary;
    private final Timer requestProcessDuration;
    private Integer bufferMaxRequestLength;
    private Integer bufferOptimalRequestLength;
    private final HttpHeaderExtractor httpHeaderExtractor;
    private final Optional<RequestAcknowledgementHandler> requestAcknowledgementHandler;
    private final Optional<HttpAcknowledgementExceptionHandler> acknowledgementExceptionHandler;
    private final int maxPendingRequests;
    private final Counter requestsRejectedPendingAcknowledgmentsCounter;
    private final AtomicInteger unfinishedAcknowledgements = new AtomicInteger();

    public LogHTTPService(final int bufferWriteTimeoutInMillis,
                          final Buffer<Record<Log>> buffer,
                          final PluginMetrics pluginMetrics,
                          final InputCodec codec,
                          final HttpHeaderExtractor httpHeaderExtractor) {
        this(bufferWriteTimeoutInMillis, buffer, pluginMetrics, codec, httpHeaderExtractor, null, null, 0);
    }

    LogHTTPService(final int bufferWriteTimeoutInMillis,
                   final Buffer<Record<Log>> buffer,
                   final PluginMetrics pluginMetrics,
                   final InputCodec codec,
                   final HttpHeaderExtractor httpHeaderExtractor,
                   final RequestAcknowledgementHandler requestAcknowledgementHandler,
                   final HttpAcknowledgementExceptionHandler acknowledgementExceptionHandler,
                   final int maxPendingRequests) {
        this.requestAcknowledgementHandler = Optional.ofNullable(requestAcknowledgementHandler);
        this.acknowledgementExceptionHandler = Optional.ofNullable(acknowledgementExceptionHandler);
        this.maxPendingRequests = maxPendingRequests;
        this.buffer = buffer;
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;
        this.bufferMaxRequestLength = buffer.getMaxRequestSize().isPresent() ? buffer.getMaxRequestSize().get(): null;
        this.bufferOptimalRequestLength = buffer.getOptimalRequestSize().isPresent() ? buffer.getOptimalRequestSize().get(): null;
        this.codec = codec;
        this.httpHeaderExtractor = httpHeaderExtractor;
        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
        successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS);
        requestsOverOptimalSizeCounter = pluginMetrics.counter(REQUESTS_OVER_OPTIMAL_SIZE);
        requestsOverMaximumSizeCounter = pluginMetrics.counter(REQUESTS_OVER_MAXIMUM_SIZE);
        payloadSizeSummary = pluginMetrics.summary(PAYLOAD_SIZE);
        requestProcessDuration = pluginMetrics.timer(REQUEST_PROCESS_DURATION);
        requestsRejectedPendingAcknowledgmentsCounter = pluginMetrics.counter(REQUESTS_REJECTED_PENDING_ACKNOWLEDGMENTS);
    }

    public LogHTTPService(final int bufferWriteTimeoutInMillis,
                          final Buffer<Record<Log>> buffer,
                          final PluginMetrics pluginMetrics,
                          final InputCodec codec) {
        this(bufferWriteTimeoutInMillis, buffer, pluginMetrics, codec, new HttpHeaderExtractor(Collections.emptySet()));
    }

    @Post
    public HttpResponse doPost(final ServiceRequestContext serviceRequestContext, final AggregatedHttpRequest aggregatedHttpRequest) throws Exception {
        requestsReceivedCounter.increment();
        payloadSizeSummary.record(aggregatedHttpRequest.content().length());

        if(serviceRequestContext.isTimedOut()) {
            return HttpResponse.of(HttpStatus.REQUEST_TIMEOUT);
        }

        if (requestAcknowledgementHandler.isPresent() && acknowledgementExceptionHandler.isPresent() && !buffer.isByteBuffer()) {
            return processAcknowledgedRequest(serviceRequestContext, aggregatedHttpRequest,
                    requestAcknowledgementHandler.get(), acknowledgementExceptionHandler.get());
        }

        return requestProcessDuration.recordCallable(() -> processRequest(aggregatedHttpRequest));
    }

    private HttpResponse processAcknowledgedRequest(final ServiceRequestContext serviceRequestContext,
                                                    final AggregatedHttpRequest aggregatedHttpRequest,
                                                    final RequestAcknowledgementHandler handler,
                                                    final HttpAcknowledgementExceptionHandler exceptionHandler) throws Exception {
        final Timer.Sample sample = Timer.start();
        serviceRequestContext.setAttr(HttpAcknowledgementExceptionHandler.REQUEST_ACCEPTED, Boolean.TRUE);
        if (unfinishedAcknowledgements.get() >= maxPendingRequests) {
            sample.stop(requestProcessDuration);
            requestsRejectedPendingAcknowledgmentsCounter.increment();
            return exceptionHandler.retryableResponse(HttpStatus.TOO_MANY_REQUESTS, PENDING_ACKNOWLEDGMENTS_MESSAGE);
        }
        final PendingRequestAcknowledgement pendingAcknowledgement;
        try {
            final HttpData content = aggregatedHttpRequest.content();
            final List<Record<Log>> records = parseRecords(content,
                    Collections.unmodifiableMap(httpHeaderExtractor.extractHeaders(aggregatedHttpRequest)));
            pendingAcknowledgement = handler.create(serviceRequestContext, records);
            unfinishedAcknowledgements.incrementAndGet();
            try {
                writeRecords(records, content);
            } catch (final Exception e) {
                unfinishedAcknowledgements.decrementAndGet();
                throw e;
            }
        } catch (final Exception e) {
            sample.stop(requestProcessDuration);
            throw e;
        }
        final CompletableFuture<Void> acknowledged = pendingAcknowledgement.complete();
        countUntilFinishedOrExpired(acknowledged, serviceRequestContext.requestTimeoutMillis() * ACKNOWLEDGEMENT_SET_EXPIRY_FACTOR);
        final RequestHeaders requestHeaders = aggregatedHttpRequest.headers();
        final CompletableFuture<HttpResponse> response = acknowledged.handle((ignored, failure) -> {
            if (serviceRequestContext.isTimedOut()) {
                return HttpResponse.of(HttpStatus.REQUEST_TIMEOUT);
            }
            sample.stop(requestProcessDuration);
            if (failure != null) {
                return exceptionHandler.handleException(serviceRequestContext, HttpRequest.of(requestHeaders), failure);
            }
            successRequestsCounter.increment();
            return HttpResponse.of(HttpStatus.OK);
        });
        return HttpResponse.of(response);
    }

    HttpResponse processRequest(final AggregatedHttpRequest aggregatedHttpRequest) throws Exception {
        final HttpData content = aggregatedHttpRequest.content();
        final Map<String, Object> extractedHeaders = Collections.unmodifiableMap(httpHeaderExtractor.extractHeaders(aggregatedHttpRequest));

        if (buffer.isByteBuffer()) {
            if (bufferMaxRequestLength != null && bufferOptimalRequestLength != null && content.array().length > bufferOptimalRequestLength) {
                jsonCodec.serializeSplit(content, this::writeChunkedBody, bufferOptimalRequestLength - SERIALIZATION_OVERHEAD);
            } else {
                try {
                    jsonCodec.validate(content);
                } catch (IOException e) {
                    LOG.error("Failed to parse the request of size {} due to: {}", content.length(), e.getMessage());
                    throw new IOException("Bad request data format. Needs to be json array.", e.getCause());
                }

                try {
                    buffer.writeBytes(content.array(), null, bufferWriteTimeoutInMillis);
                } catch (Exception e) {
                    LOG.error("Failed to write the request of size {} due to: {}", content.length(), e.getMessage());
                    throw e;
                }
            }
        } else {
            writeRecords(parseRecords(content, extractedHeaders), content);
        }

        successRequestsCounter.increment();
        return HttpResponse.of(HttpStatus.OK);
    }

    private void countUntilFinishedOrExpired(final CompletableFuture<Void> acknowledged, final long expiryMillis) {
        final CompletableFuture<Void> finished = expiryMillis > 0
                ? acknowledged.copy().orTimeout(expiryMillis, TimeUnit.MILLISECONDS)
                : acknowledged.copy();
        finished.whenComplete((ignored, failure) -> unfinishedAcknowledgements.decrementAndGet());
    }

    private void writeRecords(final List<Record<Log>> records, final HttpData content) throws Exception {
        try {
            buffer.writeAll(records, bufferWriteTimeoutInMillis);
        } catch (final Exception e) {
            LOG.error("Failed to write the request of size {} due to: {}", content.length(), e.getMessage());
            throw e;
        }
    }

    private List<Record<Log>> parseRecords(final HttpData content, final Map<String, Object> extractedHeaders) throws IOException {
        final List<String> jsonList;
        final List<Record<Log>> records = new ArrayList<>();
        if (codec != null) {
            try {
                codec.parse(content.toInputStream(), record -> {
                    records.add(new Record<>((Log) record.getData()));
                });
            } catch (IOException e) {
                LOG.error("Failed to parse the request of size {} using specified input codec {} due to: {}", content.length(), codec.getClass(), e.getMessage());
                throw new IOException("Bad request data format. ", e.getCause());
            }
        } else {

            try {
                jsonList = jsonCodec.parse(content);
            } catch (IOException e) {
                LOG.error("Failed to parse the request of size {} due to: {}", content.length(), e.getMessage());
                throw new IOException("Bad request data format. Needs to be json array.", e.getCause());
            }

            records.addAll(
                    jsonList.stream()
                            .map(this::buildRecordLog)
                            .collect(Collectors.toList())
            );
        }

        if (!extractedHeaders.isEmpty()) {
            for (Record<Log> record : records) {
                record.getData().getMetadata().setAttribute("headers", extractedHeaders);
            }
        }
        return records;
    }

    private void writeChunkedBody(final String chunk) {
        final byte[] chunkBytes = chunk.getBytes();

        if (chunkBytes.length > bufferMaxRequestLength) {
            requestsOverMaximumSizeCounter.increment();
            LOG.error("Unable to write chunked bytes of size {} as it exceeds the maximum buffer size of {}", chunkBytes.length, bufferMaxRequestLength);
            return;
        } else if (chunkBytes.length > bufferOptimalRequestLength) {
            requestsOverOptimalSizeCounter.increment();
        }

        final String key = UUID.randomUUID().toString();
        try {
            buffer.writeBytes(chunkBytes, key, bufferWriteTimeoutInMillis);
        } catch (final Exception e) {
            LOG.error("Failed to write chunked bytes of size {} due to: {}", chunkBytes.length, e.getMessage());
        }
    }

    private Record<Log> buildRecordLog(final String json) {
        final JacksonLog.Builder builder = JacksonLog.builder()
                .withData(json)
                .getThis();
        return new Record<>(builder.build());
    }
}
