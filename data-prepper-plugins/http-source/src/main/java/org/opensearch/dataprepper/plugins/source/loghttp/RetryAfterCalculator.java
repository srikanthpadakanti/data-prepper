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

import com.google.rpc.RetryInfo;
import org.opensearch.dataprepper.RetryInfoCalculator;

import java.util.Objects;

class RetryAfterCalculator {
    private static final long MINIMUM_RETRY_AFTER_SECONDS = 1;

    private final RetryInfoCalculator retryInfoCalculator;

    RetryAfterCalculator(final RetryInfoCalculator retryInfoCalculator) {
        this.retryInfoCalculator = Objects.requireNonNull(retryInfoCalculator);
    }

    String nextRetryAfter() {
        final RetryInfo retryInfo = retryInfoCalculator.createRetryInfo();
        final long roundedUpSeconds = retryInfo.getRetryDelay().getSeconds() + (retryInfo.getRetryDelay().getNanos() > 0 ? 1 : 0);
        return Long.toString(Math.max(MINIMUM_RETRY_AFTER_SECONDS, roundedUpSeconds));
    }
}
