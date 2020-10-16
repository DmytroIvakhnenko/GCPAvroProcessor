package io.github.dmytroivakhnenko.gcpavroprocessor.config;

import com.google.cloud.storage.Storage;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gcp.storage.integration.GcsRemoteFileTemplate;
import org.springframework.cloud.gcp.storage.integration.GcsSessionFactory;
import org.springframework.cloud.gcp.storage.integration.inbound.GcsStreamingMessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import java.io.IOException;
import java.io.InputStream;

@Configuration
@EnableIntegration
public class IntegrationConfig {
    private static final Logger LOG = LoggerFactory.getLogger(IntegrationConfig.class);

    @Value("${gcs.bucket.name}")
    private String bucketName;

    @Value("${avro.file.pattern}")
    private String avroFilePattern;


    @Bean
    public MessageChannel fileChannel() {
        return new DirectChannel();
    }

    /**
     * An inbound channel adapter that polls the remote directory and produces messages with {@code
     * InputStream} payload that can be used to read the data from remote files.
     *
     * @param gcs a storage client to use
     * @return a message source
     */
    @Bean
    @InboundChannelAdapter(channel = "gcs-channel", poller = @Poller(fixedDelay = "5000"))
    public MessageSource<InputStream> streamingAdapter(Storage gcs) {
        GcsStreamingMessageSource adapter = new GcsStreamingMessageSource(
                new GcsRemoteFileTemplate(new GcsSessionFactory(gcs)));
        adapter.setRemoteDirectory(bucketName);
        return adapter;
    }

    /**
     * A service activator that receives messages produced by the {@code synchronizerAdapter} and
     * simply outputs the file name of each to the console.
     *
     * @return a message handler
     */
    @Bean
    @ServiceActivator(inputChannel = "gcs-channel")
    public MessageHandler handleNewFileFromSynchronizer() {
        return (message) -> {
            LOG.info("New synchronizer");
            InputStream file = (InputStream) message.getPayload();
            try {
                LOG.info(
                        "File " + IOUtils.toString(file) + " received by the non-streaming inbound "
                                + "channel adapter.");
            } catch (IOException e) {
                e.printStackTrace();
            }
            LOG.info("After read synchronizer");
        };
    }
}
