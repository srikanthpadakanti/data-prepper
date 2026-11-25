/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor;

import io.opentelemetry.proto.trace.v1.Span.SpanKind;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;

import java.io.File;
import java.lang.reflect.Field;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ServiceMapStatefulProcessorAcknowledgementsTest {
    private static final long WINDOW_DURATION_MILLIS = 100;
    private static final long START_MILLIS = 1;
    private static final long FIRST_ROTATION_MILLIS = START_MILLIS + WINDOW_DURATION_MILLIS + 10;
    private static final long SECOND_ROTATION_MILLIS = FIRST_ROTATION_MILLIS + WINDOW_DURATION_MILLIS + 10;
    private static final int TRACE_COUNT = 20;

    @TempDir
    private File tempDir;

    private Clock clock;
    private final List<ServiceMapStatefulProcessor> processors = new ArrayList<>();
    private ExecutorService executorService;

    @BeforeEach
    void setUp() throws ReflectiveOperationException {
        resetStaticState();
        clock = mock(Clock.class);
        when(clock.instant()).thenReturn(Instant.now());
        when(clock.millis()).thenReturn(START_MILLIS);
    }

    @AfterEach
    void tearDown() {
        if (!processors.isEmpty()) {
            processors.get(0).shutdown();
        }
        if (executorService != null) {
            executorService.shutdownNow();
        }
    }

    @Test
    void doExecute_releases_spans_only_after_their_window_retires_and_relationships_are_released() {
        final ServiceMapStatefulProcessor processor = createProcessors(1).get(0);
        final CountingAcknowledgementSet acknowledgementSet = new CountingAcknowledgementSet();
        final List<Record<Event>> spans = spansIn(acknowledgementSet, 1);

        processor.doExecute(spans);
        spans.forEach(span -> span.getData().getEventHandle().release(true));

        when(clock.millis()).thenReturn(FIRST_ROTATION_MILLIS);
        final Collection<Record<Event>> relationships = processor.doExecute(Collections.emptyList());

        assertThat(relationships, not(empty()));
        assertThat(acknowledgementSet.isDone(), equalTo(false));

        relationships.forEach(relationship -> relationship.getData().getEventHandle().release(true));

        assertThat(acknowledgementSet.isDone(), equalTo(false));

        when(clock.millis()).thenReturn(SECOND_ROTATION_MILLIS);
        processor.doExecute(Collections.emptyList());

        assertThat(acknowledgementSet.isDone(), equalTo(true));
        assertThat(acknowledgementSet.getResult(), equalTo(true));
    }

    @Test
    void doExecute_retired_window_still_waits_for_unreleased_relationships() {
        final ServiceMapStatefulProcessor processor = createProcessors(1).get(0);
        final CountingAcknowledgementSet acknowledgementSet = new CountingAcknowledgementSet();
        final List<Record<Event>> spans = spansIn(acknowledgementSet, 1);

        processor.doExecute(spans);
        spans.forEach(span -> span.getData().getEventHandle().release(true));
        when(clock.millis()).thenReturn(FIRST_ROTATION_MILLIS);
        final List<Record<Event>> relationships = new ArrayList<>(processor.doExecute(Collections.emptyList()));
        when(clock.millis()).thenReturn(SECOND_ROTATION_MILLIS);
        processor.doExecute(Collections.emptyList());
        relationships.subList(1, relationships.size())
                .forEach(relationship -> relationship.getData().getEventHandle().release(true));

        assertThat(relationships, hasSize(greaterThan(1)));
        assertThat(acknowledgementSet.isDone(), equalTo(false));

        relationships.get(0).getData().getEventHandle().release(false);

        assertThat(acknowledgementSet.isDone(), equalTo(true));
        assertThat(acknowledgementSet.getResult(), equalTo(false));
    }

    @Test
    void doExecute_span_without_service_name_is_not_held() {
        final ServiceMapStatefulProcessor processor = createProcessors(1).get(0);
        final CountingAcknowledgementSet acknowledgementSet = new CountingAcknowledgementSet();
        final Span span = ServiceMapTestUtils.getSpan(null, "checkout",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), "",
                Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(16)), SpanKind.SPAN_KIND_CLIENT);
        acknowledgementSet.add(span.getEventHandle());

        processor.doExecute(Collections.singletonList(new Record<>(span)));
        span.getEventHandle().release(true);

        assertThat(acknowledgementSet.isDone(), equalTo(true));
    }

    @Test
    void doExecute_with_two_workers_holds_the_relationships_of_every_worker() throws Exception {
        final List<ServiceMapStatefulProcessor> workers = createProcessors(2);
        executorService = Executors.newFixedThreadPool(2);
        final CountingAcknowledgementSet acknowledgementSet = new CountingAcknowledgementSet();
        final List<Record<Event>> spans = spansIn(acknowledgementSet, TRACE_COUNT);

        executeTogether(workers, spans);
        spans.forEach(span -> span.getData().getEventHandle().release(true));
        when(clock.millis()).thenReturn(FIRST_ROTATION_MILLIS);
        final List<Collection<Record<Event>>> relationshipsByWorker = executeTogether(workers, Collections.emptyList());
        when(clock.millis()).thenReturn(SECOND_ROTATION_MILLIS);
        executeTogether(workers, Collections.emptyList());
        relationshipsByWorker.get(0).forEach(relationship -> relationship.getData().getEventHandle().release(true));

        assertThat(relationshipsByWorker.get(1), not(empty()));
        assertThat(acknowledgementSet.isDone(), equalTo(false));

        relationshipsByWorker.get(1).forEach(relationship -> relationship.getData().getEventHandle().release(true));

        assertThat(acknowledgementSet.isDone(), equalTo(true));
    }

    @Test
    void doExecute_emits_a_negatively_released_relationship_again() {
        final ServiceMapStatefulProcessor processor = createProcessors(1).get(0);
        final List<Record<Event>> firstSpans = spansIn(new CountingAcknowledgementSet(), 1);
        processor.doExecute(firstSpans);
        when(clock.millis()).thenReturn(FIRST_ROTATION_MILLIS);
        final Collection<Record<Event>> firstRelationships = processor.doExecute(Collections.emptyList());
        firstRelationships.forEach(relationship -> relationship.getData().getEventHandle().release(false));

        processor.doExecute(spansIn(new CountingAcknowledgementSet(), 1));
        when(clock.millis()).thenReturn(SECOND_ROTATION_MILLIS);
        final Collection<Record<Event>> secondRelationships = processor.doExecute(Collections.emptyList());

        assertThat(firstRelationships, not(empty()));
        assertThat(secondRelationships, hasSize(firstRelationships.size()));
    }

    @Test
    void shutdown_after_every_span_is_evaluated_releases_the_windows_positively() {
        final ServiceMapStatefulProcessor processor = createProcessors(1).get(0);
        final CountingAcknowledgementSet acknowledgementSet = new CountingAcknowledgementSet();
        final List<Record<Event>> spans = spansIn(acknowledgementSet, 1);

        processor.doExecute(spans);
        spans.forEach(span -> span.getData().getEventHandle().release(true));
        when(clock.millis()).thenReturn(FIRST_ROTATION_MILLIS);
        processor.doExecute(Collections.emptyList())
                .forEach(relationship -> relationship.getData().getEventHandle().release(true));

        assertThat(acknowledgementSet.isDone(), equalTo(false));

        processors.clear();
        processor.shutdown();

        assertThat(acknowledgementSet.isDone(), equalTo(true));
        assertThat(acknowledgementSet.getResult(), equalTo(true));
    }

    @Test
    void shutdown_of_every_worker_releases_the_windows_once() {
        final List<ServiceMapStatefulProcessor> workers = new ArrayList<>(createProcessors(2));
        final CountingAcknowledgementSet acknowledgementSet = new CountingAcknowledgementSet();
        final List<Record<Event>> spans = spansIn(acknowledgementSet, 1);
        workers.get(0).doExecute(spans);
        spans.forEach(span -> span.getData().getEventHandle().release(true));
        processors.clear();

        workers.forEach(ServiceMapStatefulProcessor::shutdown);

        assertThat(acknowledgementSet.isDone(), equalTo(true));
        assertThat(acknowledgementSet.getResult(), equalTo(false));
    }

    @Test
    void shutdown_with_unevaluated_spans_releases_the_windows_negatively() {
        final ServiceMapStatefulProcessor processor = createProcessors(1).get(0);
        final CountingAcknowledgementSet acknowledgementSet = new CountingAcknowledgementSet();
        final List<Record<Event>> spans = spansIn(acknowledgementSet, 1);

        processor.doExecute(spans);
        spans.forEach(span -> span.getData().getEventHandle().release(true));

        assertThat(acknowledgementSet.isDone(), equalTo(false));

        processors.clear();
        processor.shutdown();

        assertThat(acknowledgementSet.isDone(), equalTo(true));
        assertThat(acknowledgementSet.getResult(), equalTo(false));
    }

    private List<ServiceMapStatefulProcessor> createProcessors(final int workerCount) {
        for (int index = 0; index < workerCount; index++) {
            processors.add(new ServiceMapStatefulProcessor(WINDOW_DURATION_MILLIS, tempDir, clock, workerCount,
                    PluginMetrics.fromNames("serviceMap", "pipeline")));
        }
        return processors;
    }

    private List<Collection<Record<Event>>> executeTogether(final List<ServiceMapStatefulProcessor> workers,
                                                            final List<Record<Event>> firstWorkerRecords) throws Exception {
        final List<Future<Collection<Record<Event>>>> futures = new ArrayList<>();
        for (int index = 0; index < workers.size(); index++) {
            final ServiceMapStatefulProcessor worker = workers.get(index);
            final List<Record<Event>> records = index == 0 ? firstWorkerRecords : Collections.emptyList();
            futures.add(executorService.submit(() -> worker.doExecute(records)));
        }
        final List<Collection<Record<Event>>> results = new ArrayList<>();
        for (final Future<Collection<Record<Event>>> future : futures) {
            results.add(future.get(10, TimeUnit.SECONDS));
        }
        return results;
    }

    private static List<Record<Event>> spansIn(final CountingAcknowledgementSet acknowledgementSet, final int traceCount) {
        final List<Record<Event>> spans = new ArrayList<>();
        for (int trace = 0; trace < traceCount; trace++) {
            final String traceId = Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(16));
            final Span parent = ServiceMapTestUtils.getSpan("frontend", "checkout",
                    Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), "", traceId, SpanKind.SPAN_KIND_CLIENT);
            final Span child = ServiceMapTestUtils.getSpan("cart", "get_items",
                    Hex.encodeHexString(ServiceMapTestUtils.getRandomBytes(8)), parent.getSpanId(), traceId,
                    SpanKind.SPAN_KIND_SERVER);
            acknowledgementSet.add(parent.getEventHandle());
            acknowledgementSet.add(child.getEventHandle());
            spans.addAll(Arrays.asList(new Record<>(parent), new Record<>(child)));
        }
        return spans;
    }

    private static void resetStaticState() throws ReflectiveOperationException {
        for (final String fieldName : Arrays.asList("RELATIONSHIP_STATE", "processorsCreated")) {
            final Field field = ServiceMapStatefulProcessor.class.getDeclaredField(fieldName);
            field.setAccessible(true);
            final Object value = field.get(null);
            if (value instanceof Set) {
                ((Set<?>) value).clear();
            } else {
                ((AtomicInteger) value).set(0);
            }
            field.setAccessible(false);
        }
    }
}
