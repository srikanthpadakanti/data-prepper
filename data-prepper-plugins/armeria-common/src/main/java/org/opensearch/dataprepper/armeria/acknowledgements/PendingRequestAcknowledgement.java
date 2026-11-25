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

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * The acknowledgement state of one push request.
 */
public class PendingRequestAcknowledgement {
    private final AcknowledgementSet acknowledgementSet;
    private final CompletableFuture<Void> result;

    PendingRequestAcknowledgement(final AcknowledgementSet acknowledgementSet, final CompletableFuture<Void> result) {
        this.acknowledgementSet = Objects.requireNonNull(acknowledgementSet);
        this.result = Objects.requireNonNull(result);
    }

    /**
     * Completes the acknowledgement set. Call it only after the records are written to the buffer.
     *
     * @return a future that completes when every event is acknowledged, or completes exceptionally with a
     * NegativeAcknowledgementException when any event is not
     */
    public CompletableFuture<Void> complete() {
        acknowledgementSet.complete();
        return result;
    }
}
