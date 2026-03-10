/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

import com.linecorp.armeria.server.Server;
import org.opensearch.dataprepper.HttpRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.http.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.server.CreateServer;
import org.opensearch.dataprepper.plugins.server.ServerConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

/**
 * A Data Prepper source plugin that receives Prometheus Remote Write requests.
 * This plugin acts as a Remote Write receiver, accepting metrics pushed from Prometheus
 * servers or other Remote Write compatible clients.
 *
 * <p>Configuration example:</p>
 * <pre>
 * prometheus-source:
 *   port: 9090
 *   path: /api/v1/write
 *   ssl: true
 *   ssl_certificate_file: /path/to/cert.pem
 *   ssl_key_file: /path/to/key.pem
 *   output_format: OTEL
 * </pre>
 */
@DataPrepperPlugin(name = "prometheus", pluginType = Source.class, pluginConfigurationType = PrometheusRemoteWriteSourceConfig.class)
public class PrometheusRemoteWriteSource implements Source<Record<Event>> {

    private static final String PLUGIN_NAME = "prometheus";
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusRemoteWriteSource.class);
    static final String SERVER_CONNECTIONS = "serverConnections";

    private final PrometheusRemoteWriteSourceConfig sourceConfig;
    private final CertificateProviderFactory certificateProviderFactory;
    private final ArmeriaHttpAuthenticationProvider authenticationProvider;
    private final HttpRequestExceptionHandler httpRequestExceptionHandler;
    private final String pipelineName;
    private final PluginMetrics pluginMetrics;
    private Server server;

    @DataPrepperPluginConstructor
    public PrometheusRemoteWriteSource(final PrometheusRemoteWriteSourceConfig sourceConfig,
                                        final PluginMetrics pluginMetrics,
                                        final PluginFactory pluginFactory,
                                        final PipelineDescription pipelineDescription) {
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.certificateProviderFactory = new CertificateProviderFactory(sourceConfig);

        final PluginModel authenticationConfiguration = sourceConfig.getAuthentication();
        final PluginSetting authenticationPluginSetting;

        if (authenticationConfiguration == null || ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME
                .equals(authenticationConfiguration.getPluginName())) {
            LOG.warn("Creating Prometheus Remote Write source without authentication. This is not secure.");
            LOG.warn("In order to set up authentication, configure the 'authentication' option.");
        }

        if (authenticationConfiguration != null) {
            authenticationPluginSetting = new PluginSetting(
                    authenticationConfiguration.getPluginName(),
                    authenticationConfiguration.getPluginSettings());
        } else {
            authenticationPluginSetting = new PluginSetting(
                    ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME,
                    Collections.emptyMap());
        }
        authenticationPluginSetting.setPipelineName(pipelineName);
        authenticationProvider = pluginFactory.loadPlugin(ArmeriaHttpAuthenticationProvider.class, authenticationPluginSetting);
        httpRequestExceptionHandler = new HttpRequestExceptionHandler(pluginMetrics);
    }

    @Override
    public void start(final Buffer<Record<Event>> buffer) {
        if (buffer == null) {
            throw new IllegalStateException("Buffer provided is null");
        }

        if (server == null) {
            final ServerConfiguration serverConfiguration = convertConfiguration(sourceConfig);
            final CreateServer createServer = new CreateServer(
                    serverConfiguration, LOG, pluginMetrics, PLUGIN_NAME, pipelineName);

            final PrometheusRemoteWriteService remoteWriteService = new PrometheusRemoteWriteService(
                    serverConfiguration.getBufferTimeoutInMillis(),
                    buffer,
                    pluginMetrics,
                    sourceConfig);

            server = createServer.createHTTPServer(
                    null,
                    certificateProviderFactory,
                    authenticationProvider,
                    httpRequestExceptionHandler,
                    remoteWriteService);

            pluginMetrics.gauge(SERVER_CONNECTIONS, server, Server::numConnections);
        }

        try {
            server.start().get();
        } catch (ExecutionException ex) {
            if (ex.getCause() != null && ex.getCause() instanceof RuntimeException) {
                throw (RuntimeException) ex.getCause();
            } else {
                throw new RuntimeException(ex);
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }

        LOG.info("Started Prometheus Remote Write source on port {} with path {}",
                sourceConfig.getPort(), sourceConfig.getPath());
    }

    @Override
    public void stop() {
        if (server != null) {
            try {
                server.stop().get();
            } catch (ExecutionException ex) {
                if (ex.getCause() != null && ex.getCause() instanceof RuntimeException) {
                    throw (RuntimeException) ex.getCause();
                } else {
                    throw new RuntimeException(ex);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
            server = null;
        }
        LOG.info("Stopped Prometheus Remote Write source.");
    }

    /**
     * Converts the source configuration to a ServerConfiguration.
     *
     * @param config the source configuration
     * @return the server configuration
     */
    private static ServerConfiguration convertConfiguration(final PrometheusRemoteWriteSourceConfig config) {
        final ServerConfiguration serverConfiguration = new ServerConfiguration();
        serverConfiguration.setPath(config.getPath());
        serverConfiguration.setHealthCheck(config.hasHealthCheckService());
        serverConfiguration.setRequestTimeoutInMillis(config.getRequestTimeoutInMillis());
        serverConfiguration.setCompression(config.getCompression());
        serverConfiguration.setAuthentication(config.getAuthentication());
        serverConfiguration.setSsl(config.isSsl());
        serverConfiguration.setUnauthenticatedHealthCheck(config.isUnauthenticatedHealthCheck());
        serverConfiguration.setMaxRequestLength(config.getMaxRequestLength());
        serverConfiguration.setPort(config.getPort());
        serverConfiguration.setThreadCount(config.getThreadCount());
        serverConfiguration.setMaxPendingRequests(config.getMaxPendingRequests());
        serverConfiguration.setMaxConnectionCount(config.getMaxConnectionCount());
        serverConfiguration.setBufferTimeoutInMillis(config.getBufferTimeoutInMillis());
        return serverConfiguration;
    }
}
