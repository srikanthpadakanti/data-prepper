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

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.InternalEventHandle;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

class WindowAcknowledgements {
    private final List<Window> windows;

    WindowAcknowledgements(final int windowCount) {
        if (windowCount < 1) {
            throw new IllegalArgumentException("windowCount must be at least 1");
        }
        this.windows = new ArrayList<>(windowCount);
        for (int index = 0; index < windowCount; index++) {
            windows.add(new Window());
        }
    }

    synchronized void attachToNewestWindow(final Event event) {
        final EventHandle eventHandle = event.getEventHandle();
        if (eventHandle instanceof InternalEventHandle && ((InternalEventHandle) eventHandle).hasAcknowledgementSet()) {
            ((InternalEventHandle) eventHandle).acquireReference();
            windows.get(windows.size() - 1).hold(eventHandle);
        }
    }

    synchronized void holdUntilReleased(final Collection<Record<Event>> records) {
        final List<Window> windowsHoldingEvents = windows.stream()
                .filter(Window::holdsEvents)
                .collect(Collectors.toList());
        if (windowsHoldingEvents.isEmpty()) {
            return;
        }
        final List<EventHandle> emittedHandles = records.stream()
                .map(record -> record.getData().getEventHandle())
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        if (emittedHandles.isEmpty()) {
            return;
        }
        final Emission emission = new Emission(windowsHoldingEvents, emittedHandles.size());
        for (final EventHandle emittedHandle : emittedHandles) {
            final AtomicBoolean released = new AtomicBoolean(false);
            emittedHandle.onRelease((handle, result) -> {
                if (!result) {
                    emission.fail();
                }
                if (released.compareAndSet(false, true)) {
                    emission.countDown();
                }
            });
        }
    }

    synchronized void rotate() {
        windows.remove(0).release();
        windows.add(new Window());
    }

    synchronized void releaseAll(final boolean result) {
        for (int index = 0; index < windows.size(); index++) {
            final Window window = windows.set(index, new Window());
            if (!result) {
                window.fail();
            }
            window.release();
        }
    }

    private static class Window {
        private final List<EventHandle> heldHandles = new ArrayList<>();
        private final AtomicInteger pending = new AtomicInteger(1);
        private volatile boolean failed;

        synchronized void hold(final EventHandle eventHandle) {
            heldHandles.add(eventHandle);
        }

        synchronized boolean holdsEvents() {
            return !heldHandles.isEmpty();
        }

        void retain() {
            pending.incrementAndGet();
        }

        void fail() {
            failed = true;
        }

        void release() {
            if (pending.decrementAndGet() == 0) {
                releaseHeldHandles();
            }
        }

        private synchronized void releaseHeldHandles() {
            final boolean result = !failed;
            heldHandles.forEach(eventHandle -> eventHandle.release(result));
            heldHandles.clear();
        }
    }

    private static class Emission {
        private final List<Window> windows;
        private final AtomicInteger remaining;

        Emission(final List<Window> windows, final int emittedCount) {
            this.windows = windows;
            this.remaining = new AtomicInteger(emittedCount);
            windows.forEach(Window::retain);
        }

        void fail() {
            windows.forEach(Window::fail);
        }

        void countDown() {
            if (remaining.decrementAndGet() == 0) {
                windows.forEach(Window::release);
            }
        }
    }
}
