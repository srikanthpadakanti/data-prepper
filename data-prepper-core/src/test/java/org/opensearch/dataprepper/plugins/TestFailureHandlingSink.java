/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;

import java.util.Collection;

@DataPrepperPlugin(name = "test_failure_handling_sink", pluginType = Sink.class)
public class TestFailureHandlingSink implements Sink<Record<String>> {
    @Override
    public void output(final Collection<Record<String>> records) {
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void initialize() {
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public boolean handlesFailedEvents() {
        return true;
    }
}
