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

import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.Map;

@DataPrepperPlugin(name = "test_byte_buffer", pluginType = Buffer.class)
public class TestByteBuffer implements Buffer<Record<String>> {
    @Override
    public void write(final Record<String> record, final int timeoutInMillis) {
    }

    @Override
    public void writeAll(final Collection<Record<String>> records, final int timeoutInMillis) {
    }

    @Override
    public Map.Entry<Collection<Record<String>>, CheckpointState> read(final int timeoutInMillis) {
        return null;
    }

    @Override
    public void checkpoint(final CheckpointState checkpointState) {
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public boolean isByteBuffer() {
        return true;
    }
}
