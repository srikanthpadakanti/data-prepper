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
import org.opensearch.dataprepper.model.peerforwarder.RequiresPeerForwarding;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.Collections;

@DataPrepperPlugin(name = "test_peer_forwarding_processor", pluginType = Processor.class)
public class TestPeerForwardingProcessor implements Processor<Record<String>, Record<String>>, RequiresPeerForwarding {
    @Override
    public Collection<Record<String>> execute(final Collection<Record<String>> records) {
        return records;
    }

    @Override
    public void prepareForShutdown() {
    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public Collection<String> getIdentificationKeys() {
        return Collections.singletonList("key");
    }
}
