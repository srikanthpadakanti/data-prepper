/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.core.parser;

import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.sink.Sink;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static java.lang.String.format;

/**
 * Collects the pipeline facts that the strict acknowledgement validation needs while pipelines are built,
 * and reports the problems for a pipeline and every pipeline downstream of it.
 */
class StrictAcknowledgementValidator {
    private static final String PROCESSOR_PROBLEM_FORMAT = "Processor [%s] in pipeline [%s] requires peer forwarding or holds events, " +
            "so it acknowledges events before a sink writes them. Remove it or disable acknowledgments.";
    private static final String BUFFER_PROBLEM_FORMAT = "Buffer [%s] in pipeline [%s] is a byte buffer, " +
            "which cannot track acknowledgments for the source. Use an event buffer or disable acknowledgments.";
    private static final String SINK_PROBLEM_FORMAT = "Sink [%s] in pipeline [%s] does not send events it cannot write " +
            "to the failure pipeline or to a DLQ. Use a sink that does or disable acknowledgments.";
    private static final String FAILURE_PIPELINE_PROBLEM_FORMAT = "Acknowledgments in pipeline [%s] require the failure pipeline [%s] " +
            "without a source. Define it or disable acknowledgments.";

    private final Map<String, Set<String>> problemsByPipeline = new HashMap<>();
    private final Map<String, List<String>> downstreamPipelinesByPipeline = new HashMap<>();

    void recordPipeline(final String pipelineName, final List<String> downstreamPipelineNames) {
        Objects.requireNonNull(pipelineName);
        Objects.requireNonNull(downstreamPipelineNames);
        problemsByPipeline.put(pipelineName, new LinkedHashSet<>());
        downstreamPipelinesByPipeline.put(pipelineName, new ArrayList<>(downstreamPipelineNames));
    }

    void recordBuffer(final String pipelineName, final String bufferName, final Buffer buffer) {
        if (buffer.isByteBuffer()) {
            problemsFor(pipelineName).add(format(BUFFER_PROBLEM_FORMAT, bufferName, pipelineName));
        }
    }

    void recordProcessor(final String pipelineName, final String processorName, final Processor processor) {
        findProcessorProblem(pipelineName, processorName, processor).ifPresent(problemsFor(pipelineName)::add);
    }

    Optional<String> findProcessorProblem(final String pipelineName, final String processorName, final Processor processor) {
        if (processor instanceof RequiresPeerForwarding || processor.holdsEvents()) {
            return Optional.of(format(PROCESSOR_PROBLEM_FORMAT, processorName, pipelineName));
        }
        return Optional.empty();
    }

    void recordSink(final String pipelineName, final String sinkName, final Sink sink) {
        if (!sink.handlesFailedEvents()) {
            problemsFor(pipelineName).add(format(SINK_PROBLEM_FORMAT, sinkName, pipelineName));
        }
    }

    List<String> findProblems(final String pipelineName, final String failurePipelineName, final boolean failurePipelineExists) {
        final List<String> problems = new ArrayList<>();
        if (!failurePipelineExists) {
            problems.add(format(FAILURE_PIPELINE_PROBLEM_FORMAT, pipelineName, failurePipelineName));
        }
        final Set<String> visited = new HashSet<>();
        final Deque<String> toVisit = new ArrayDeque<>();
        toVisit.add(pipelineName);
        while (!toVisit.isEmpty()) {
            final String current = toVisit.poll();
            if (visited.add(current)) {
                problems.addAll(problemsByPipeline.getOrDefault(current, Collections.emptySet()));
                toVisit.addAll(downstreamPipelinesByPipeline.getOrDefault(current, Collections.emptyList()));
            }
        }
        return problems;
    }

    private Set<String> problemsFor(final String pipelineName) {
        return problemsByPipeline.computeIfAbsent(pipelineName, name -> new LinkedHashSet<>());
    }
}
