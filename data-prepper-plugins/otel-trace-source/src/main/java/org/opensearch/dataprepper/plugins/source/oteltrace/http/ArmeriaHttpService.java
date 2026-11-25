package org.opensearch.dataprepper.plugins.source.oteltrace.http;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.opensearch.dataprepper.armeria.acknowledgements.PendingRequestAcknowledgement;
import org.opensearch.dataprepper.armeria.acknowledgements.RequestAcknowledgementHandler;
import org.opensearch.dataprepper.exceptions.BadRequestException;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.logging.DataPrepperMarkers;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.Consumes;
import com.linecorp.armeria.server.annotation.Post;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;

public class ArmeriaHttpService {
    private static final Logger LOG = LoggerFactory.getLogger(ArmeriaHttpService.class);

    public static final String REQUEST_TIMEOUTS = "requestTimeouts";
    public static final String REQUESTS_RECEIVED = "requestsReceived";
    public static final String BAD_REQUESTS = "badRequests";
    public static final String REQUESTS_TOO_LARGE = "requestsTooLarge";
    public static final String INTERNAL_SERVER_ERROR = "internalServerError";
    public static final String SUCCESS_REQUESTS = "successRequests";
    public static final String PAYLOAD_SIZE = "payloadSize";
    public static final String REQUEST_PROCESS_DURATION = "requestProcessDuration";

    private final OTelProtoCodec.OTelProtoDecoder oTelProtoDecoder;
    private final Buffer<Record<Object>> buffer;

    private static final CompletableFuture<Void> COMPLETED = CompletableFuture.completedFuture(null);

    private final int bufferWriteTimeoutInMillis;
    private final Optional<RequestAcknowledgementHandler> requestAcknowledgementHandler;

    private final Counter requestsReceivedCounter;
    private final Counter successRequestsCounter;
    private final DistributionSummary payloadSizeSummary;
    private final Timer requestProcessDuration;

    public ArmeriaHttpService(Buffer<Record<Object>> buffer, final OTelProtoCodec.OTelProtoDecoder otelProtoDecoder, final PluginMetrics pluginMetrics, final int bufferWriteTimeoutInMillis) {
        this(buffer, otelProtoDecoder, pluginMetrics, bufferWriteTimeoutInMillis, Optional.empty());
    }

    public ArmeriaHttpService(final Buffer<Record<Object>> buffer,
                              final OTelProtoCodec.OTelProtoDecoder otelProtoDecoder,
                              final PluginMetrics pluginMetrics,
                              final int bufferWriteTimeoutInMillis,
                              final Optional<RequestAcknowledgementHandler> requestAcknowledgementHandler) {
        this.requestAcknowledgementHandler = Objects.requireNonNull(requestAcknowledgementHandler);
        this.buffer = buffer;
        this.oTelProtoDecoder = otelProtoDecoder;
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;

        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
        successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS);
        payloadSizeSummary = pluginMetrics.summary(PAYLOAD_SIZE);
        requestProcessDuration = pluginMetrics.timer(REQUEST_PROCESS_DURATION);
    }

    // todo make path configurable
    @Post("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
    @Consumes(value = "application/json")
    public CompletableFuture<ExportTraceServiceResponse> exportTrace(ExportTraceServiceRequest request) {
        requestsReceivedCounter.increment();
        payloadSizeSummary.record(request.getSerializedSize());

        final AtomicReference<CompletableFuture<Void>> acknowledged = new AtomicReference<>(COMPLETED);
        requestProcessDuration.record(() -> acknowledged.set(processRequest(request)));

        return acknowledged.get().thenApply(ignored -> ExportTraceServiceResponse.newBuilder().build());
    }

    private CompletableFuture<Void> processRequest(final ExportTraceServiceRequest request) {
        final Collection<Span> spans;

        try {
            spans = oTelProtoDecoder.parseExportTraceServiceRequest(request, Instant.now());
        } catch (final Exception e) {
            LOG.warn(DataPrepperMarkers.SENSITIVE, "Failed to parse request with error '{}'. Request body: {}.", e.getMessage(), request);
            throw new BadRequestException(e.getMessage(), e);
        }

        final boolean byteBuffer = buffer.isByteBuffer();
        final List<Record<Object>> records = byteBuffer
                ? Collections.emptyList()
                : spans.stream().map(span -> new Record<Object>(span)).collect(Collectors.toList());
        final Optional<PendingRequestAcknowledgement> pendingAcknowledgement = byteBuffer
                ? Optional.empty()
                : requestAcknowledgementHandler.map(handler -> handler.create(ServiceRequestContext.current(), records));
        final CompletableFuture<Void> acknowledged;
        try {
            if (byteBuffer) {
                Map<String, ExportTraceServiceRequest> requestsMap = oTelProtoDecoder.splitExportTraceServiceRequestByTraceId(request);
                for (Map.Entry<String, ExportTraceServiceRequest> entry: requestsMap.entrySet()) {
                    buffer.writeBytes(entry.getValue().toByteArray(), entry.getKey(), bufferWriteTimeoutInMillis);
                }
            } else {
                buffer.writeAll(records, bufferWriteTimeoutInMillis);
            }
            acknowledged = pendingAcknowledgement.map(PendingRequestAcknowledgement::complete).orElse(null);
        } catch (final Exception e) {
            if (ServiceRequestContext.current().isTimedOut()) {
                LOG.warn("Exception writing to buffer but request already timed out.", e);
                return COMPLETED;
            }

            LOG.error("Failed to write the request of size {} bytes due to:", request.getSerializedSize(), e);
            throw new BufferWriteException(e.getMessage(), e);
        }

        if (ServiceRequestContext.current().isTimedOut()) {
            LOG.warn("Buffer write completed successfully but request already timed out.");
            return COMPLETED;
        }

        if (acknowledged != null) {
            final ServiceRequestContext requestContext = ServiceRequestContext.current();
            return acknowledged.thenRun(() -> {
                if (!requestContext.isTimedOut()) {
                    successRequestsCounter.increment();
                }
            });
        }

        successRequestsCounter.increment();
        return COMPLETED;
    }
}
