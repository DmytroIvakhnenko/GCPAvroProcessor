package io.github.dmytroivakhnenko.gcpavroprocessor.service.impl;


import com.google.api.client.util.Value;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.*;
import com.google.common.util.concurrent.ListenableFuture;
import example.gcp.Client;
import io.github.dmytroivakhnenko.gcpavroprocessor.config.BigQueryIntegrationConfig;
import io.github.dmytroivakhnenko.gcpavroprocessor.service.GCSFileProcessorService;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class GCSFileProcessorServiceImpl implements GCSFileProcessorService {
    private static final Logger LOG = LoggerFactory.getLogger(GCSFileProcessorServiceImpl.class);
    private static Storage storage = StorageOptions.getDefaultInstance().getService();
    private BigQueryIntegrationConfig.BigQueryFileGateway bigQueryFileGateway;

    @Value("${bigquery.tableName.full}")
    private String tableNameFull;

    public GCSFileProcessorServiceImpl(BigQueryIntegrationConfig.BigQueryFileGateway bigQueryFileGateway) {
        this.bigQueryFileGateway = bigQueryFileGateway;
    }

    @Override
    public void processFile(BlobInfo blobInfo) {
        var blob = storage.get(BlobId.of(blobInfo.getBucket(), blobInfo.getName()));

        LOG.info(
                "File " + new String(blob.getContent()) + " received by the non-streaming inbound "
                        + "channel adapter.");
        String gcsPath = String.format("gs://%s/%s", blobInfo.getBucket(), blobInfo.getName());
        loadAvroFromGcs(gcsPath);

        //var client = deserialize(blob);
        //LOG.info("Deserialized file: " + client.toString());
    }

    @Override
    public ListenableFuture<Job> processFileViaIntegration(BlobInfo blobInfo) {
        var blob = storage.get(BlobId.of(blobInfo.getBucket(), blobInfo.getName()));

        LOG.info(
                "File " + new String(blob.getContent()) + " received by the non-streaming inbound "
                        + "channel adapter.");
        String gcsPath = String.format("gs://%s/%s", blobInfo.getBucket(), blobInfo.getName());
        return bigQueryFileGateway.writeToBigQueryTable(blob.getContent(), tableNameFull);

    }

    private Client deserialize(Blob blob) {
        //DeSerializing the objects
        DatumReader<Client> empDatumReader = new SpecificDatumReader<>(Client.class);

        Decoder decoder = DecoderFactory.get().binaryDecoder(blob.getContent(), null);
        Client client = null;
        try {
            client = empDatumReader.read(null, decoder);
        } catch (IOException e) {
            LOG.error("Exception occurs during avro file deserialization", e);
        }
        return client;
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
