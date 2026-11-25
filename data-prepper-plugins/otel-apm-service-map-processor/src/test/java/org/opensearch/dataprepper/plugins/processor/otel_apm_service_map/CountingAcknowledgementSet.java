/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map;

import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSet;
import org.opensearch.dataprepper.model.acknowledgements.ProgressCheck;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.InternalEventHandle;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

class CountingAcknowledgementSet implements AcknowledgementSet {
    private final Map<EventHandle, Integer> pending = new HashMap<>();
    private boolean result = true;

    @Override
    public void add(final EventHandle eventHandle) {
        ((InternalEventHandle) eventHandle).addAcknowledgementSet(this);
        pending.put(eventHandle, 1);
    }

    @Override
    public void acquire(final EventHandle eventHandle) {
        pending.computeIfPresent(eventHandle, (handle, count) -> count + 1);
    }

    @Override
    public boolean release(final EventHandle eventHandle, final boolean releaseResult) {
        result = result && releaseResult;
        final Integer count = pending.get(eventHandle);
        if (count == null) {
            return false;
        }
        if (count == 1) {
            pending.remove(eventHandle);
        } else {
            pending.put(eventHandle, count - 1);
        }
        return true;
    }

    boolean isDone() {
        return pending.isEmpty();
    }

    boolean getResult() {
        return result;
    }

    @Override
    public void complete() {
    }

    @Override
    public void addProgressCheck(final Consumer<ProgressCheck> progressCheckCallback, final Duration progressCheckInterval) {
    }

    @Override
    public void increaseExpiry(final Duration expiryIncreaseTime) {
    }

    @Override
    public Instant getExpirationTime() {
        return Instant.MAX;
    }

    @Override
    public void cancel() {
    }
}
