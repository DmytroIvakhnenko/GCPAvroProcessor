package io.github.dmytroivakhnenko.gcpavroprocessor.service.impl;


import com.google.cloud.bigquery.*;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import example.gcp.Client;
import io.github.dmytroivakhnenko.gcpavroprocessor.config.BigQueryIntegrationConfig;
import io.github.dmytroivakhnenko.gcpavroprocessor.exception.AvroFileValidationException;
import io.github.dmytroivakhnenko.gcpavroprocessor.service.GCSFileProcessorService;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

@Service
public class GCSFileProcessorServiceImpl implements GCSFileProcessorService {
    private static final Logger LOG = LoggerFactory.getLogger(GCSFileProcessorServiceImpl.class);
    private static final Storage storage = StorageOptions.getDefaultInstance().getService();
    private final BigQueryIntegrationConfig.BigQueryFileGateway bigQueryFileGateway;

    //@Value("${bigquery.tableName.full}")
    private final String tableNameFull = "client_full";

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

        LOG.info("File " + new String(blob.getContent()) + " received by the non-streaming inbound channel adapter");
        if (!isValidatAvroFileAccordingToSchema(blob.getContent(), Client.SCHEMA$)) {
            throw new AvroFileValidationException(String.format("File %s/%s is not following avro schema", blobInfo.getBucket(), blobInfo.getName()));
        }
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

    boolean isValidatAvroFileAccordingToSchema(byte[] avroFileContent, Schema schema) {
        try (InputStream input = new ByteArrayInputStream(avroFileContent)) {
            DatumReader<Client> reader = new SpecificDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(input, null);
            reader.read(null, decoder);
        } catch (AvroTypeException | IOException e) {
            LOG.error("Exception occurs during avro file validation", e);
            return false;
        }
        return true;
    }
}
