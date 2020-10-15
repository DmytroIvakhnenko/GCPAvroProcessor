package io.github.dmytroivakhnenko.gcpavroprocessor.config;

import com.google.cloud.storage.Storage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gcp.storage.integration.inbound.GcsInboundFileSynchronizer;
import org.springframework.cloud.gcp.storage.integration.inbound.GcsInboundFileSynchronizingMessageSource;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.messaging.MessageHandler;

import java.io.File;
import java.nio.file.Paths;

@Configuration
@EnableIntegration
public class IntegrationConfig {
    private static final Logger LOG = LoggerFactory.getLogger(IntegrationConfig.class);

    @Value("${gcs.bucket.name}")
    private String bucketName;

    @Value("${avro.file.pattern}")
    private String avroFilePattern;

    /**
     * A file synchronizer that knows how to connect to the remote file system (GCS) and scan
     * it for new files and then download the files.
     *
     * @param gcs a storage client to use
     * @return an inbound file synchronizer
     */
    @Bean
    public GcsInboundFileSynchronizer gcsInboundFileSynchronizer(Storage gcs) {
        GcsInboundFileSynchronizer synchronizer = new GcsInboundFileSynchronizer(gcs);
        synchronizer.setRemoteDirectory(this.bucketName);

        return synchronizer;
    }

    /**
     * An inbound channel adapter that polls the GCS bucket for new files and copies them to
     * the local filesystem. The resulting message source produces messages containing handles
     * to local files.
     *
     * @param synchronizer an inbound file synchronizer
     * @return a message source
     */
    @Bean
    @InboundChannelAdapter(channel = "new-file-channel", poller = @Poller(fixedDelay = "5000"))
    public MessageSource<File> synchronizerAdapter(GcsInboundFileSynchronizer synchronizer) {
        GcsInboundFileSynchronizingMessageSource syncAdapter = new GcsInboundFileSynchronizingMessageSource(
                synchronizer);
        syncAdapter.setLocalDirectory(Paths.get("").toFile());

        return syncAdapter;
    }

    /**
     * A service activator that receives messages produced by the {@code synchronizerAdapter}
     * and simply outputs the file name of each to the console.
     *
     * @return a message handler
     */
    @Bean
    @ServiceActivator(inputChannel = "new-file-channel")
    public MessageHandler handleNewFileFromSynchronizer() {
        return (message) -> {
            LOG.info("Enter synchronizer");
            File file = (File) message.getPayload();
            LOG.info("File " + file.getName() + " received by the non-streaming inbound "
                    + "channel adapter.");
        };
    }

}
