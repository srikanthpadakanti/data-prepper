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
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;

@DataPrepperPlugin(name = "test_strict_acknowledgement_source", pluginType = Source.class)
public class TestStrictAcknowledgementSource implements Source<Record<String>> {
    @Override
    public void start(final Buffer<Record<String>> buffer) {
    }

    @Override
    public void stop() {
    }

    @Override
    public boolean areAcknowledgementsEnabled() {
        return true;
    }

    @Override
    public boolean requiresStrictAcknowledgementValidation() {
        return true;
    }
}
