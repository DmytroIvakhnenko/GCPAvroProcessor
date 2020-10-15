package io.github.dmytroivakhnenko.gcpavroprocessor.config;

import com.google.cloud.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gcp.storage.integration.GcsRemoteFileTemplate;
import org.springframework.cloud.gcp.storage.integration.GcsSessionFactory;
import org.springframework.cloud.gcp.storage.integration.filters.GcsSimplePatternFileListFilter;
import org.springframework.cloud.gcp.storage.integration.inbound.GcsStreamingMessageSource;
import org.springframework.cloud.gcp.storage.integration.outbound.GcsMessageHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.file.FileHeaders;
import org.springframework.messaging.MessageHandler;

import java.io.InputStream;

@Configuration
public class IntegrationConfig {
    private static final Logger LOG = LoggerFactory.getLogger(IntegrationConfig.class);

    @Value("${gcs.bucket.name}")
    private String bucketName;

    @Value("${avro.file.pattern}")
    private String avroFilePattern;

    /**
     * An inbound channel adapter that polls the remote directory and produces messages with
     * {@code InputStream} payload that can be used to read the data from remote files.
     *
     * @param gcs a storage client to use
     * @return a message source
     */
    @Bean
    @InboundChannelAdapter(channel = "streamingChannel", poller = @Poller(fixedDelay = "5000"))
    public MessageSource<InputStream> streamingAdapter(Storage gcs) {
        GcsStreamingMessageSource adapter = new GcsStreamingMessageSource(
                new GcsRemoteFileTemplate(new GcsSessionFactory(gcs)));
        adapter.setFilter(new GcsSimplePatternFileListFilter(avroFilePattern));
        return adapter;
    }

    /**
     * A service activator that connects to a channel with messages containing
     * {@code InputStream} payloads and copies the file data to a remote directory on GCS.
     *
     * @param gcs a storage client to use
     * @return a message handler
     */
    @Bean
    @ServiceActivator(inputChannel = "streamingChannel")
    public MessageHandler outboundChannelAdapter(Storage gcs) {
        GcsMessageHandler outboundChannelAdapter = new GcsMessageHandler(new GcsSessionFactory(gcs));

        outboundChannelAdapter
                .setFileNameGenerator((message) -> {
                    LOG.info(message.getHeaders().get(FileHeaders.FILENAME, String.class));
                    return message.getHeaders().get(FileHeaders.REMOTE_FILE, String.class);
                });
        return outboundChannelAdapter;
    }

}
