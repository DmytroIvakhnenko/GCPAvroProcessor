package io.github.dmytroivakhnenko.gcpavroprocessor.service.impl;


import com.google.cloud.bigquery.*;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.github.dmytroivakhnenko.gcpavroprocessor.config.BigQueryIntegrationConfig;
import io.github.dmytroivakhnenko.gcpavroprocessor.service.GCSFileProcessorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

@Service
public class GCSFileProcessorServiceImpl implements GCSFileProcessorService {
    private static final Logger LOG = LoggerFactory.getLogger(GCSFileProcessorServiceImpl.class);
    private static Storage storage = StorageOptions.getDefaultInstance().getService();
    private BigQueryIntegrationConfig.BigQueryFileGateway bigQueryFileGateway;

    //@Value("${bigquery.tableName.full}")
    private String tableNameFull = "client_full";

    public GCSFileProcessorServiceImpl(BigQueryIntegrationConfig.BigQueryFileGateway bigQueryFileGateway) {
        this.bigQueryFileGateway = bigQueryFileGateway;
    }

/*
    @Override
    public void processFile(BlobInfo blobInfo) {
        var blob = storage.get(BlobId.of(blobInfo.getBucket(), blobInfo.getName()));

        LOG.info(
                "File " + new String(blob.getContent()) + " received by the non-streaming inbound "
                        + "channel adapter.");
        String gcsPath = String.format("gs://%s/%s", blobInfo.getBucket(), blobInfo.getName());
        loadAvroFromGcs(gcsPath);
    }
*/

    @Override
    public ListenableFuture<Job> processFileViaIntegration(BlobInfo blobInfo) {
        var blob = storage.get(BlobId.of(blobInfo.getBucket(), blobInfo.getName()));

        LOG.info(
                "File " + new String(blob.getContent()) + " received by the non-streaming inbound "
                        + "channel adapter.");
        return bigQueryFileGateway.writeToBigQueryTable(blob.getContent(), tableNameFull);

    }

    private void loadAvroFromGcs(String sourceUri) {
        var datasetName = "clients_dataset";
        var tableName = "client_full";
        try {
            // Initialize client that will be used to send requests. This client only needs to be created
            // once, and can be reused for multiple requests.
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            TableId tableId = TableId.of(datasetName, tableName);
            LoadJobConfiguration loadConfig = LoadJobConfiguration.of(tableId, sourceUri, FormatOptions.avro());

            // Load data from a GCS Avro file into the table
            Job job = bigquery.create(JobInfo.of(loadConfig));
            // Blocks until this load table job completes its execution, either failing or succeeding.
            job = job.waitFor();
            if (job.isDone()) {
                LOG.info("Avro from GCS successfully loaded in a table");
            } else {
                LOG.error(
                        "BigQuery was unable to load into the table due to an error:"
                                + job.getStatus().getError());
            }
        } catch (BigQueryException | InterruptedException e) {
            LOG.error("Column not added during load append \n" + e.toString());
        }
    }
}
