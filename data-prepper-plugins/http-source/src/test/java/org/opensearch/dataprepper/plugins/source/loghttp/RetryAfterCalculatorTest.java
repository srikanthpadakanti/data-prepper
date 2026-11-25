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

import com.google.protobuf.Duration;
import com.google.rpc.RetryInfo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.opensearch.dataprepper.RetryInfoCalculator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RetryAfterCalculatorTest {
    @ParameterizedTest
    @CsvSource({"0, 0, 1", "0, 100000000, 1", "1, 0, 1", "1, 500000000, 2", "2, 0, 2"})
    void nextRetryAfter_rounds_the_delay_up_to_whole_seconds(final long seconds, final int nanos, final String expected) {
        final RetryInfoCalculator retryInfoCalculator = mock(RetryInfoCalculator.class);
        when(retryInfoCalculator.createRetryInfo()).thenReturn(RetryInfo.newBuilder()
                .setRetryDelay(Duration.newBuilder().setSeconds(seconds).setNanos(nanos).build())
                .build());

        assertThat(new RetryAfterCalculator(retryInfoCalculator).nextRetryAfter(), equalTo(expected));
    }

    @Test
    void constructor_with_null_calculator_throws() {
        assertThrows(NullPointerException.class, () -> new RetryAfterCalculator(null));
    }
}
