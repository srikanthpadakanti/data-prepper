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

import com.linecorp.armeria.server.ServiceRequestContext;
import io.grpc.stub.StreamObserver;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.exceptions.NegativeAcknowledgementException;
import org.opensearch.dataprepper.exceptions.SourceShuttingDownException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Holds the response to a push request until every event in the request is acknowledged by the sinks.
 * The request deadline is the server request timeout; a set that is still pending at the deadline is left to
 * expire without a callback.
 */
public class RequestAcknowledgementHandler {
    private static final Logger LOG = LoggerFactory.getLogger(RequestAcknowledgementHandler.class);

    public static final String REQUESTS_ACKNOWLEDGED = "requestsAcknowledged";
    public static final String REQUESTS_NEGATIVELY_ACKNOWLEDGED = "requestsNegativelyAcknowledged";
    public static final String REQUESTS_REJECTED_SHUTTING_DOWN = "requestsRejectedShuttingDown";
    public static final String REQUESTS_PENDING_ACKNOWLEDGEMENT = "requestsPendingAcknowledgement";
    private static final String NEGATIVE_ACKNOWLEDGEMENT_MESSAGE = "The events in the request were not written to the sinks. Retry the request.";
    private static final String SHUTTING_DOWN_MESSAGE = "The source is shutting down. Retry the request.";

    private final AcknowledgementSetManager acknowledgementSetManager;
    private final Duration requestTimeout;
    private final Counter requestsAcknowledgedCounter;
    private final Counter requestsNegativelyAcknowledgedCounter;
    private final Counter requestsRejectedShuttingDownCounter;
    private final AtomicInteger pendingRequests;
    private final Object pendingRequestsLock;
    private volatile boolean shuttingDown;

    /**
     * Creates a handler.
     *
     * @param acknowledgementSetManager the manager that creates one acknowledgement set per request
     * @param requestTimeout the server request timeout, used as the acknowledgement set expiry
     * @param pluginMetrics the metrics of the source
     */
    public RequestAcknowledgementHandler(final AcknowledgementSetManager acknowledgementSetManager,
                                         final Duration requestTimeout,
                                         final PluginMetrics pluginMetrics) {
        this.acknowledgementSetManager = Objects.requireNonNull(acknowledgementSetManager);
        this.requestTimeout = Objects.requireNonNull(requestTimeout);
        Objects.requireNonNull(pluginMetrics);
        requestsAcknowledgedCounter = pluginMetrics.counter(REQUESTS_ACKNOWLEDGED);
        requestsNegativelyAcknowledgedCounter = pluginMetrics.counter(REQUESTS_NEGATIVELY_ACKNOWLEDGED);
        requestsRejectedShuttingDownCounter = pluginMetrics.counter(REQUESTS_REJECTED_SHUTTING_DOWN);
        pendingRequests = new AtomicInteger();
        pendingRequestsLock = new Object();
        pluginMetrics.gauge(REQUESTS_PENDING_ACKNOWLEDGEMENT, pendingRequests, AtomicInteger::get);
    }

    /**
     * Creates the acknowledgement set for one request and adds every event of the request to it.
     * The request counts as pending until its response is sent.
     *
     * @param requestContext the context of the request
     * @param records the records the request writes to the buffer
     * @return the pending acknowledgement; call {@link PendingRequestAcknowledgement#complete()} after the buffer write
     * @throws SourceShuttingDownException if the source is shutting down
     */
    public PendingRequestAcknowledgement create(final ServiceRequestContext requestContext,
                                                final Collection<? extends Record<?>> records) {
        pendingRequests.incrementAndGet();
        if (shuttingDown) {
            decrementPendingRequests();
            requestsRejectedShuttingDownCounter.increment();
            throw new SourceShuttingDownException(SHUTTING_DOWN_MESSAGE);
        }
        requestContext.log().whenComplete().thenRun(this::decrementPendingRequests);

        final CompletableFuture<Void> result = new CompletableFuture<>();
        final AcknowledgementSet acknowledgementSet = acknowledgementSetManager.create(
                success -> onAcknowledgement(result, success), requestTimeout, false);
        for (final Record<?> record : records) {
            if (record.getData() instanceof Event) {
                acknowledgementSet.add((Event) record.getData());
            }
        }
        return new PendingRequestAcknowledgement(acknowledgementSet, result);
    }

    /**
     * Sends the gRPC response once the request is acknowledged, or the error once it is negatively acknowledged.
     * Nothing is sent if the request already timed out.
     *
     * @param acknowledged the future returned by {@link PendingRequestAcknowledgement#complete()}
     * @param requestContext the context of the request
     * @param responseObserver the response observer of the call
     * @param response the response to send on success
     * @param onSuccess run before the response is sent on success
     * @param <T> the response type
     */
    public <T> void respondWhenAcknowledged(final CompletableFuture<Void> acknowledged,
                                            final ServiceRequestContext requestContext,
                                            final StreamObserver<T> responseObserver,
                                            final T response,
                                            final Runnable onSuccess) {
        acknowledged.whenComplete((ignored, failure) -> {
            if (requestContext.isTimedOut()) {
                LOG.debug("Acknowledgement received after the request timed out.");
                return;
            }
            if (failure == null) {
                onSuccess.run();
                responseObserver.onNext(response);
                responseObserver.onCompleted();
            } else {
                responseObserver.onError(failure);
            }
        });
    }

    /**
     * Rejects new requests from now on.
     */
    public void prepareForShutdown() {
        shuttingDown = true;
    }

    /**
     * Waits until no request is pending or the timeout passes.
     *
     * @param timeout the longest time to wait
     * @return true if no request is pending
     */
    public boolean awaitPendingRequests(final Duration timeout) {
        final long deadline = System.nanoTime() + timeout.toNanos();
        synchronized (pendingRequestsLock) {
            while (pendingRequests.get() > 0) {
                final long remaining = deadline - System.nanoTime();
                if (remaining <= 0) {
                    LOG.warn("{} requests were still waiting for acknowledgements at shutdown.", pendingRequests.get());
                    return false;
                }
                try {
                    TimeUnit.NANOSECONDS.timedWait(pendingRequestsLock, remaining);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return true;
    }

    private void onAcknowledgement(final CompletableFuture<Void> result, final boolean success) {
        if (success) {
            requestsAcknowledgedCounter.increment();
            result.complete(null);
        } else {
            requestsNegativelyAcknowledgedCounter.increment();
            result.completeExceptionally(new NegativeAcknowledgementException(NEGATIVE_ACKNOWLEDGEMENT_MESSAGE));
        }
    }

    private void decrementPendingRequests() {
        if (pendingRequests.decrementAndGet() == 0) {
            synchronized (pendingRequestsLock) {
                pendingRequestsLock.notifyAll();
            }
        }
    }
}
