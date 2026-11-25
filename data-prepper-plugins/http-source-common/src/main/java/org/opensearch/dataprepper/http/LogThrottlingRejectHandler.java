/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.http;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.ResponseHeaders;
import com.linecorp.armeria.common.ResponseHeadersBuilder;
import com.linecorp.armeria.server.Service;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.throttling.ThrottlingRejectHandler;
import io.micrometer.core.instrument.Counter;

import javax.annotation.Nullable;
import java.util.Optional;
import java.util.function.Supplier;

public class LogThrottlingRejectHandler implements ThrottlingRejectHandler<HttpRequest, HttpResponse> {
    public static final String REQUESTS_REJECTED = "requestsRejected";

    private final int maxPendingRequests;
    private final Counter rejectedRequestsCounter;
    private final Optional<Supplier<String>> retryAfterSupplier;

    public LogThrottlingRejectHandler(final int maxPendingRequests, final PluginMetrics pluginMetrics) {
        this(maxPendingRequests, pluginMetrics, null);
    }

    /**
     * Creates a reject handler that adds a Retry-After header to every rejection when a supplier is given.
     *
     * @param maxPendingRequests the configured max_pending_requests
     * @param pluginMetrics the metrics of the source
     * @param retryAfterSupplier supplies the Retry-After value in seconds, or null for no header
     */
    public LogThrottlingRejectHandler(final int maxPendingRequests, final PluginMetrics pluginMetrics,
                                      final Supplier<String> retryAfterSupplier) {
        this.maxPendingRequests = maxPendingRequests;
        rejectedRequestsCounter = pluginMetrics.counter(REQUESTS_REJECTED);
        this.retryAfterSupplier = Optional.ofNullable(retryAfterSupplier);
    }

    @Override
    public HttpResponse handleRejected(final Service<HttpRequest, HttpResponse> delegate, final ServiceRequestContext ctx,
                                       final HttpRequest req, final @Nullable Throwable cause) throws Exception {
        rejectedRequestsCounter.increment();
        final ResponseHeadersBuilder headersBuilder = ResponseHeaders.builder(HttpStatus.TOO_MANY_REQUESTS)
                .contentType(MediaType.ANY_TYPE);
        retryAfterSupplier.ifPresent(supplier -> headersBuilder.add(HttpHeaderNames.RETRY_AFTER, supplier.get()));
        return HttpResponse.of(headersBuilder.build(), HttpData.ofUtf8(String.format(
                "The number of pending requests in the work queue reaches max_pending_requests:%d. Please retry later",
                maxPendingRequests)));
    }
}
