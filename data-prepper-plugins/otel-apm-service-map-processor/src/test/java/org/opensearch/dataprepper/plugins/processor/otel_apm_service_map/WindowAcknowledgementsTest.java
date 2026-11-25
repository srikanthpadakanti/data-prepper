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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class WindowAcknowledgementsTest {
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3})
    void attachToNewestWindow_span_is_pending_until_its_window_retires(final int windowCount) {
        final WindowAcknowledgements windowAcknowledgements = new WindowAcknowledgements(windowCount);
        final CountingAcknowledgementSet acknowledgementSet = new CountingAcknowledgementSet();
        final Event span = eventIn(acknowledgementSet);

        windowAcknowledgements.attachToNewestWindow(span);
        span.getEventHandle().release(true);

        for (int rotation = 1; rotation < windowCount; rotation++) {
            windowAcknowledgements.rotate();
            assertThat(acknowledgementSet.isDone(), equalTo(false));
        }
        windowAcknowledgements.rotate();

        assertThat(acknowledgementSet.isDone(), equalTo(true));
        assertThat(acknowledgementSet.getResult(), equalTo(true));
    }

    @Test
    void attachToNewestWindow_span_attached_after_rotation_is_in_the_new_window() {
        final WindowAcknowledgements windowAcknowledgements = new WindowAcknowledgements(2);
        final CountingAcknowledgementSet acknowledgementSet = new CountingAcknowledgementSet();
        windowAcknowledgements.rotate();
        final Event span = eventIn(acknowledgementSet);

        windowAcknowledgements.attachToNewestWindow(span);
        span.getEventHandle().release(true);
        windowAcknowledgements.rotate();

        assertThat(acknowledgementSet.isDone(), equalTo(false));

        windowAcknowledgements.rotate();

        assertThat(acknowledgementSet.isDone(), equalTo(true));
    }

    @Test
    void attachToNewestWindow_span_without_acknowledgement_handle_is_ignored() {
        final WindowAcknowledgements windowAcknowledgements = new WindowAcknowledgements(2);
        final Event span = mock(Event.class);
        final EventHandle eventHandle = mock(EventHandle.class);
        when(span.getEventHandle()).thenReturn(eventHandle);

        windowAcknowledgements.attachToNewestWindow(span);

        verifyNoInteractions(eventHandle);
    }

    @Test
    void holdUntilReleased_set_is_pending_until_every_emitted_event_is_released() {
        final WindowAcknowledgements windowAcknowledgements = new WindowAcknowledgements(2);
        final CountingAcknowledgementSet acknowledgementSet = new CountingAcknowledgementSet();
        final Event span = eventIn(acknowledgementSet);
        windowAcknowledgements.attachToNewestWindow(span);
        span.getEventHandle().release(true);
        final Event firstEmitted = emittedEvent();
        final Event secondEmitted = emittedEvent();

        windowAcknowledgements.holdUntilReleased(List.of(new Record<>(firstEmitted), new Record<>(secondEmitted)));
        windowAcknowledgements.rotate();
        windowAcknowledgements.rotate();

        assertThat(acknowledgementSet.isDone(), equalTo(false));

        firstEmitted.getEventHandle().release(true);

        assertThat(acknowledgementSet.isDone(), equalTo(false));

        secondEmitted.getEventHandle().release(true);

        assertThat(acknowledgementSet.isDone(), equalTo(true));
        assertThat(acknowledgementSet.getResult(), equalTo(true));
    }

    @Test
    void holdUntilReleased_links_emitted_events_to_every_live_window() {
        final WindowAcknowledgements windowAcknowledgements = new WindowAcknowledgements(2);
        final CountingAcknowledgementSet olderSet = new CountingAcknowledgementSet();
        final CountingAcknowledgementSet newerSet = new CountingAcknowledgementSet();
        final Event olderSpan = eventIn(olderSet);
        windowAcknowledgements.attachToNewestWindow(olderSpan);
        windowAcknowledgements.rotate();
        final Event newerSpan = eventIn(newerSet);
        windowAcknowledgements.attachToNewestWindow(newerSpan);
        olderSpan.getEventHandle().release(true);
        newerSpan.getEventHandle().release(true);
        final Event emitted = emittedEvent();

        windowAcknowledgements.holdUntilReleased(List.of(new Record<>(emitted)));
        windowAcknowledgements.rotate();
        windowAcknowledgements.rotate();

        assertThat(olderSet.isDone(), equalTo(false));
        assertThat(newerSet.isDone(), equalTo(false));

        emitted.getEventHandle().release(true);

        assertThat(olderSet.isDone(), equalTo(true));
        assertThat(newerSet.isDone(), equalTo(true));
    }

    @Test
    void holdUntilReleased_negative_release_of_an_emitted_event_fails_the_set() {
        final WindowAcknowledgements windowAcknowledgements = new WindowAcknowledgements(2);
        final CountingAcknowledgementSet acknowledgementSet = new CountingAcknowledgementSet();
        final Event span = eventIn(acknowledgementSet);
        windowAcknowledgements.attachToNewestWindow(span);
        span.getEventHandle().release(true);
        final Event emitted = emittedEvent();

        windowAcknowledgements.holdUntilReleased(List.of(new Record<>(emitted)));
        windowAcknowledgements.rotate();
        windowAcknowledgements.rotate();
        emitted.getEventHandle().release(false);

        assertThat(acknowledgementSet.isDone(), equalTo(true));
        assertThat(acknowledgementSet.getResult(), equalTo(false));
    }

    @Test
    void holdUntilReleased_second_release_of_the_same_event_is_ignored() {
        final WindowAcknowledgements windowAcknowledgements = new WindowAcknowledgements(2);
        final CountingAcknowledgementSet acknowledgementSet = new CountingAcknowledgementSet();
        final Event span = eventIn(acknowledgementSet);
        windowAcknowledgements.attachToNewestWindow(span);
        span.getEventHandle().release(true);
        final Event firstEmitted = emittedEvent();
        final Event secondEmitted = emittedEvent();

        windowAcknowledgements.holdUntilReleased(List.of(new Record<>(firstEmitted), new Record<>(secondEmitted)));
        windowAcknowledgements.rotate();
        windowAcknowledgements.rotate();
        firstEmitted.getEventHandle().release(true);
        firstEmitted.getEventHandle().release(true);

        assertThat(acknowledgementSet.isDone(), equalTo(false));
    }

    @Test
    void holdUntilReleased_without_acknowledgement_sets_does_not_touch_the_events() {
        final WindowAcknowledgements windowAcknowledgements = new WindowAcknowledgements(2);
        final Event emitted = mock(Event.class);

        windowAcknowledgements.holdUntilReleased(List.of(new Record<>(emitted)));

        verifyNoInteractions(emitted);
    }

    @Test
    void holdUntilReleased_with_no_events_does_not_hold_the_set() {
        final WindowAcknowledgements windowAcknowledgements = new WindowAcknowledgements(2);
        final CountingAcknowledgementSet acknowledgementSet = new CountingAcknowledgementSet();
        final Event span = eventIn(acknowledgementSet);
        windowAcknowledgements.attachToNewestWindow(span);
        span.getEventHandle().release(true);

        windowAcknowledgements.holdUntilReleased(Collections.emptyList());
        windowAcknowledgements.rotate();
        windowAcknowledgements.rotate();

        assertThat(acknowledgementSet.isDone(), equalTo(true));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void releaseAll_releases_every_window_with_the_result(final boolean result) {
        final WindowAcknowledgements windowAcknowledgements = new WindowAcknowledgements(3);
        final CountingAcknowledgementSet olderSet = new CountingAcknowledgementSet();
        final CountingAcknowledgementSet newerSet = new CountingAcknowledgementSet();
        final Event olderSpan = eventIn(olderSet);
        windowAcknowledgements.attachToNewestWindow(olderSpan);
        windowAcknowledgements.rotate();
        final Event newerSpan = eventIn(newerSet);
        windowAcknowledgements.attachToNewestWindow(newerSpan);
        olderSpan.getEventHandle().release(true);
        newerSpan.getEventHandle().release(true);

        windowAcknowledgements.releaseAll(result);

        assertThat(olderSet.isDone(), equalTo(true));
        assertThat(newerSet.isDone(), equalTo(true));
        assertThat(olderSet.getResult(), equalTo(result));
        assertThat(newerSet.getResult(), equalTo(result));
    }

    @Test
    void holdUntilReleased_skips_emitted_events_without_a_handle() {
        final WindowAcknowledgements windowAcknowledgements = new WindowAcknowledgements(2);
        final CountingAcknowledgementSet acknowledgementSet = new CountingAcknowledgementSet();
        final Event span = eventIn(acknowledgementSet);
        windowAcknowledgements.attachToNewestWindow(span);
        span.getEventHandle().release(true);
        final Event emitted = mock(Event.class);
        when(emitted.getEventHandle()).thenReturn(null);

        windowAcknowledgements.holdUntilReleased(List.of(new Record<>(emitted)));
        windowAcknowledgements.rotate();
        windowAcknowledgements.rotate();

        assertThat(acknowledgementSet.isDone(), equalTo(true));
    }

    @Test
    void rotate_completes_every_one_of_many_sets_held_by_the_window() {
        final WindowAcknowledgements windowAcknowledgements = new WindowAcknowledgements(1);
        final List<CountingAcknowledgementSet> acknowledgementSets = new ArrayList<>();
        for (int index = 0; index < 1000; index++) {
            final CountingAcknowledgementSet acknowledgementSet = new CountingAcknowledgementSet();
            final Event span = eventIn(acknowledgementSet);
            windowAcknowledgements.attachToNewestWindow(span);
            span.getEventHandle().release(true);
            acknowledgementSets.add(acknowledgementSet);
        }

        windowAcknowledgements.rotate();

        assertThat(acknowledgementSets.stream().allMatch(CountingAcknowledgementSet::isDone), equalTo(true));
    }

    @Test
    void holdUntilReleased_negative_release_after_a_positive_release_fails_the_set() {
        final WindowAcknowledgements windowAcknowledgements = new WindowAcknowledgements(2);
        final CountingAcknowledgementSet acknowledgementSet = new CountingAcknowledgementSet();
        final Event span = eventIn(acknowledgementSet);
        windowAcknowledgements.attachToNewestWindow(span);
        span.getEventHandle().release(true);
        final Event firstEmitted = emittedEvent();
        final Event secondEmitted = emittedEvent();

        windowAcknowledgements.holdUntilReleased(List.of(new Record<>(firstEmitted), new Record<>(secondEmitted)));
        firstEmitted.getEventHandle().release(true);
        firstEmitted.getEventHandle().release(false);
        secondEmitted.getEventHandle().release(true);
        windowAcknowledgements.rotate();
        windowAcknowledgements.rotate();

        assertThat(acknowledgementSet.isDone(), equalTo(true));
        assertThat(acknowledgementSet.getResult(), equalTo(false));
    }

    @Test
    void constructor_with_no_windows_throws() {
        assertThrows(IllegalArgumentException.class, () -> new WindowAcknowledgements(0));
    }

    private static Event eventIn(final CountingAcknowledgementSet acknowledgementSet) {
        final Event event = emittedEvent();
        acknowledgementSet.add(event.getEventHandle());
        return event;
    }

    private static Event emittedEvent() {
        return JacksonEvent.builder()
                .withEventType("event")
                .withData(Map.of("key", "value"))
                .build();
    }
}
