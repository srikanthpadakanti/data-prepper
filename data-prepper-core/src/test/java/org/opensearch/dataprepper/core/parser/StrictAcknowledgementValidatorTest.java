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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.sink.Sink;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

@ExtendWith(MockitoExtension.class)
class StrictAcknowledgementValidatorTest {

    @Mock
    private Buffer buffer;

    @Mock
    private Processor processor;

    @Mock
    private Sink sink;

    private static final String PROCESSOR_PROBLEM_FORMAT = "Processor [%s] in pipeline [%s] requires peer forwarding or holds events, " +
            "so it acknowledges events before a sink writes them. Remove it or disable acknowledgments.";
    private static final String BUFFER_PROBLEM_FORMAT = "Buffer [%s] in pipeline [%s] is a byte buffer, " +
            "which cannot track acknowledgments for the source. Use an event buffer or disable acknowledgments.";
    private static final String SINK_PROBLEM_FORMAT = "Sink [%s] in pipeline [%s] does not send events it cannot write " +
            "to the failure pipeline or to a DLQ. Use a sink that does or disable acknowledgments.";
    private static final String FAILURE_PIPELINE_PROBLEM_FORMAT = "Acknowledgments in pipeline [%s] require the failure pipeline [%s] " +
            "without a source. Define it or disable acknowledgments.";

    private String pipelineName;
    private String downstreamPipelineName;
    private String failurePipelineName;
    private String pluginName;
    private StrictAcknowledgementValidator objectUnderTest;

    @BeforeEach
    void setUp() {
        pipelineName = UUID.randomUUID().toString();
        downstreamPipelineName = UUID.randomUUID().toString();
        failurePipelineName = UUID.randomUUID().toString();
        pluginName = UUID.randomUUID().toString();
        objectUnderTest = new StrictAcknowledgementValidator();
    }

    @Test
    void findProblems_with_valid_pipeline_returns_no_problems() {
        when(buffer.isByteBuffer()).thenReturn(false);
        when(processor.holdsEvents()).thenReturn(false);
        when(sink.handlesFailedEvents()).thenReturn(true);
        objectUnderTest.recordPipeline(pipelineName, Collections.emptyList());
        objectUnderTest.recordBuffer(pipelineName, pluginName, buffer);
        objectUnderTest.recordProcessor(pipelineName, pluginName, processor);
        objectUnderTest.recordSink(pipelineName, pluginName, sink);

        assertThat(objectUnderTest.findProblems(pipelineName, failurePipelineName, true), empty());
    }

    @Test
    void findProblems_without_failure_pipeline_returns_failure_pipeline_problem() {
        objectUnderTest.recordPipeline(pipelineName, Collections.emptyList());

        assertThat(objectUnderTest.findProblems(pipelineName, failurePipelineName, false),
                contains(format(FAILURE_PIPELINE_PROBLEM_FORMAT, pipelineName, failurePipelineName)));
    }

    @Test
    void findProblems_with_byte_buffer_returns_buffer_problem() {
        when(buffer.isByteBuffer()).thenReturn(true);
        objectUnderTest.recordPipeline(pipelineName, Collections.emptyList());
        objectUnderTest.recordBuffer(pipelineName, pluginName, buffer);

        assertThat(objectUnderTest.findProblems(pipelineName, failurePipelineName, true),
                contains(format(BUFFER_PROBLEM_FORMAT, pluginName, pipelineName)));
    }

    @Test
    void findProblems_with_processor_holding_events_returns_processor_problem() {
        when(processor.holdsEvents()).thenReturn(true);
        objectUnderTest.recordPipeline(pipelineName, Collections.emptyList());
        objectUnderTest.recordProcessor(pipelineName, pluginName, processor);

        assertThat(objectUnderTest.findProblems(pipelineName, failurePipelineName, true),
                contains(format(PROCESSOR_PROBLEM_FORMAT, pluginName, pipelineName)));
    }

    @Test
    void findProblems_with_peer_forwarding_processor_returns_processor_problem_once_per_processor_name() {
        final Processor peerForwardingProcessor = mock(Processor.class, withSettings().extraInterfaces(RequiresPeerForwarding.class));
        objectUnderTest.recordPipeline(pipelineName, Collections.emptyList());
        objectUnderTest.recordProcessor(pipelineName, pluginName, peerForwardingProcessor);
        objectUnderTest.recordProcessor(pipelineName, pluginName, peerForwardingProcessor);

        assertThat(objectUnderTest.findProblems(pipelineName, failurePipelineName, true),
                contains(format(PROCESSOR_PROBLEM_FORMAT, pluginName, pipelineName)));
    }

    @Test
    void findProcessorProblem_with_processor_holding_events_returns_problem() {
        when(processor.holdsEvents()).thenReturn(true);

        assertThat(objectUnderTest.findProcessorProblem(pipelineName, pluginName, processor),
                equalTo(Optional.of(format(PROCESSOR_PROBLEM_FORMAT, pluginName, pipelineName))));
    }

    @Test
    void findProcessorProblem_with_processor_not_holding_events_returns_empty() {
        when(processor.holdsEvents()).thenReturn(false);

        assertThat(objectUnderTest.findProcessorProblem(pipelineName, pluginName, processor), equalTo(Optional.empty()));
    }

    @Test
    void findProblems_with_sink_not_handling_failed_events_returns_sink_problem() {
        when(sink.handlesFailedEvents()).thenReturn(false);
        objectUnderTest.recordPipeline(pipelineName, Collections.emptyList());
        objectUnderTest.recordSink(pipelineName, pluginName, sink);

        assertThat(objectUnderTest.findProblems(pipelineName, failurePipelineName, true),
                contains(format(SINK_PROBLEM_FORMAT, pluginName, pipelineName)));
    }

    @Test
    void findProblems_includes_problems_of_downstream_pipelines() {
        when(sink.handlesFailedEvents()).thenReturn(false);
        objectUnderTest.recordPipeline(pipelineName, List.of(downstreamPipelineName));
        objectUnderTest.recordPipeline(downstreamPipelineName, Collections.emptyList());
        objectUnderTest.recordSink(downstreamPipelineName, pluginName, sink);

        assertThat(objectUnderTest.findProblems(pipelineName, failurePipelineName, true),
                contains(format(SINK_PROBLEM_FORMAT, pluginName, downstreamPipelineName)));
    }

    @Test
    void findProblems_does_not_include_problems_of_unconnected_pipelines() {
        when(sink.handlesFailedEvents()).thenReturn(false);
        objectUnderTest.recordPipeline(pipelineName, Collections.emptyList());
        objectUnderTest.recordPipeline(downstreamPipelineName, Collections.emptyList());
        objectUnderTest.recordSink(downstreamPipelineName, pluginName, sink);

        assertThat(objectUnderTest.findProblems(pipelineName, failurePipelineName, true), empty());
    }

    @Test
    void recordPipeline_again_clears_previously_recorded_problems() {
        when(sink.handlesFailedEvents()).thenReturn(false);
        objectUnderTest.recordPipeline(pipelineName, Collections.emptyList());
        objectUnderTest.recordSink(pipelineName, pluginName, sink);
        objectUnderTest.recordPipeline(pipelineName, Collections.emptyList());

        assertThat(objectUnderTest.findProblems(pipelineName, failurePipelineName, true), empty());
    }

    @Test
    void findProblems_with_cycle_in_downstream_pipelines_visits_each_pipeline_once() {
        when(sink.handlesFailedEvents()).thenReturn(false);
        objectUnderTest.recordPipeline(pipelineName, List.of(downstreamPipelineName));
        objectUnderTest.recordPipeline(downstreamPipelineName, List.of(pipelineName));
        objectUnderTest.recordSink(downstreamPipelineName, pluginName, sink);

        assertThat(objectUnderTest.findProblems(pipelineName, failurePipelineName, true),
                contains(format(SINK_PROBLEM_FORMAT, pluginName, downstreamPipelineName)));
    }
}
