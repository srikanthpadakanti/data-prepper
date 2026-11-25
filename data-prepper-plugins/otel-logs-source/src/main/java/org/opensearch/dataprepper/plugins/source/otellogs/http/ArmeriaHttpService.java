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

import java.time.Instant;
import java.util.Collections;
import java.util.List;
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
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.ConsumesJson;
import com.linecorp.armeria.server.annotation.ConsumesProtobuf;
import com.linecorp.armeria.server.annotation.Post;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;

public class ArmeriaHttpService {
    private static final Logger LOG = LoggerFactory.getLogger(ArmeriaHttpService.class);

    public static final String REQUESTS_RECEIVED = "requestsReceived";
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

    public ArmeriaHttpService(
            Buffer<Record<Object>> buffer,
            final PluginMetrics pluginMetrics,
            final int bufferWriteTimeoutInMillis,
            final OTelProtoCodec.OTelProtoDecoder oTelProtoDecoder
    ) {
        this(buffer, pluginMetrics, bufferWriteTimeoutInMillis, oTelProtoDecoder, Optional.empty());
    }

    public ArmeriaHttpService(
            Buffer<Record<Object>> buffer,
            final PluginMetrics pluginMetrics,
            final int bufferWriteTimeoutInMillis,
            final OTelProtoCodec.OTelProtoDecoder oTelProtoDecoder,
            final Optional<RequestAcknowledgementHandler> requestAcknowledgementHandler
    ) {
        this.requestAcknowledgementHandler = Objects.requireNonNull(requestAcknowledgementHandler);
        this.buffer = buffer;
        this.oTelProtoDecoder = oTelProtoDecoder;
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;

        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
        successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS);
        payloadSizeSummary = pluginMetrics.summary(PAYLOAD_SIZE);
        requestProcessDuration = pluginMetrics.timer(REQUEST_PROCESS_DURATION);
    }

    // no path provided. Will be set by config.
    @Post("")
    @ConsumesJson
    @ConsumesProtobuf
    public CompletableFuture<ExportLogsServiceResponse> exportLog(ExportLogsServiceRequest request) {
        requestsReceivedCounter.increment();
        payloadSizeSummary.record(request.getSerializedSize());

        final AtomicReference<CompletableFuture<Void>> acknowledged = new AtomicReference<>(COMPLETED);
        requestProcessDuration.record(() -> acknowledged.set(processRequest(request)));

        return acknowledged.get().thenApply(ignored -> ExportLogsServiceResponse.newBuilder().build());
    }

    private CompletableFuture<Void> processRequest(final ExportLogsServiceRequest request) {
        final List<OpenTelemetryLog> logs;

        try {
            logs = oTelProtoDecoder.parseExportLogsServiceRequest(request, Instant.now());
        } catch (Exception e) {
            LOG.warn(DataPrepperMarkers.SENSITIVE, "Failed to parse the request with error {}. Request body: {}", e, request);
            throw new BadRequestException(e.getMessage(), e);
        }

        final boolean byteBuffer = buffer.isByteBuffer();
        final List<Record<Object>> records = byteBuffer
                ? Collections.emptyList()
                : logs.stream().map(log -> new Record<Object>(log)).collect(Collectors.toList());
        final Optional<PendingRequestAcknowledgement> pendingAcknowledgement = byteBuffer
                ? Optional.empty()
                : requestAcknowledgementHandler.map(handler -> handler.create(ServiceRequestContext.current(), records));
        final CompletableFuture<Void> acknowledged;
        try {
            if (byteBuffer) {
                buffer.writeBytes(request.toByteArray(), null, bufferWriteTimeoutInMillis);
            } else {
                buffer.writeAll(records, bufferWriteTimeoutInMillis);
            }
            acknowledged = pendingAcknowledgement.map(PendingRequestAcknowledgement::complete).orElse(null);
        } catch (Exception e) {
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
