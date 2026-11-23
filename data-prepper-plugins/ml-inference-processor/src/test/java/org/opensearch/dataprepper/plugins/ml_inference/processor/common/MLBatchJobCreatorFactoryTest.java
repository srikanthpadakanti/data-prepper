/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.ml_inference.processor.common;

import io.micrometer.core.instrument.Counter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig;
import org.opensearch.dataprepper.plugins.ml_inference.processor.configuration.ServiceName;
import org.opensearch.dataprepper.plugins.ml_inference.processor.dlq.DlqPushHandler;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.ml_inference.processor.MLProcessorConfig.DEFAULT_RETRY_WINDOW;

@ExtendWith(MockitoExtension.class)
class MLBatchJobCreatorFactoryTest {
    @Mock
    private MLProcessorConfig mlProcessorConfig;

    @Mock
    private AwsCredentialsSupplier awsCredentialsSupplier;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private DlqPushHandler dlqPushHandler;

    @Mock
    private Counter counter;

    @BeforeEach
    void setUp() {
        when(mlProcessorConfig.getRetryTimeWindow()).thenReturn(DEFAULT_RETRY_WINDOW);
        when(mlProcessorConfig.getRetryInterval()).thenReturn(Duration.ofSeconds(60));
        when(pluginMetrics.counter(anyString())).thenReturn(counter);
    }

    @Test
    void getJobCreator_for_bedrock_returns_bedrock_job_creator() {
        final MLBatchJobCreator jobCreator = MLBatchJobCreatorFactory.getJobCreator(
                ServiceName.BEDROCK, mlProcessorConfig, awsCredentialsSupplier, pluginMetrics, dlqPushHandler);

        assertThat(jobCreator, instanceOf(BedrockBatchJobCreator.class));
    }

    @Test
    void getJobCreator_called_twice_returns_a_new_job_creator_for_each_processor() {
        final MLBatchJobCreator first = MLBatchJobCreatorFactory.getJobCreator(
                ServiceName.BEDROCK, mlProcessorConfig, awsCredentialsSupplier, pluginMetrics, dlqPushHandler);
        final MLBatchJobCreator second = MLBatchJobCreatorFactory.getJobCreator(
                ServiceName.BEDROCK, mlProcessorConfig, awsCredentialsSupplier, pluginMetrics, dlqPushHandler);

        assertThat(second, not(sameInstance(first)));
    }
}
