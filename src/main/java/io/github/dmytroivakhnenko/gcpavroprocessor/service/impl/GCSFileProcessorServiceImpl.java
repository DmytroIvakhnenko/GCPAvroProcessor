package io.github.dmytroivakhnenko.gcpavroprocessor.service.impl;

import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.Job;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import example.gcp.Client;
import io.github.dmytroivakhnenko.gcpavroprocessor.config.BigQueryIntegrationConfig;
import io.github.dmytroivakhnenko.gcpavroprocessor.exception.AvroFileValidationException;
import io.github.dmytroivakhnenko.gcpavroprocessor.service.GCSFileProcessorService;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Service
public class GCSFileProcessorServiceImpl implements GCSFileProcessorService {
    private static final Logger LOG = LoggerFactory.getLogger(GCSFileProcessorServiceImpl.class);
    private static final Storage storage = StorageOptions.getDefaultInstance().getService();
    private final BigQueryIntegrationConfig.BigQueryFileGateway bigQueryFileGateway;

    @Value("${bigquery.tableName.full}")
    private String tableNameFull;

    @Value("${bigquery.tableName.mandatory}")
    private String tableNameMandatory;

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
        //TODO: filter
        return bigQueryFileGateway.writeToBigQueryTable(blob.getContent(), tableNameFull);

    }

/*    private void loadAvroFromGcs(String sourceUri) {
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
    }*/

    public List<Client> getClientsFromAvroFile(byte[] avroFileContent, BlobInfo blobInfo) {
        var clients = new ArrayList<Client>();
        DatumReader<Client> reader = new SpecificDatumReader<>(Client.class);
        try (DataFileStream<Client> dataFileReader = new DataFileStream<>(new ByteArrayInputStream(avroFileContent), reader)) {
            while (dataFileReader.hasNext()) {
                clients.add(dataFileReader.next());
            }
        } catch (IOException e) {
            var msg = String.format("Exception occurs during getting clients from avro file: %s/%s ", blobInfo.getBucket(), blobInfo.getName());
            LOG.error(String.format(msg, e));
            throw new AvroFileValidationException(msg);
        }
        return clients;
    }

    public InputStream getClientsStreamFromAvroFile(byte[] avroFileContent, BlobInfo blobInfo) {
        //var clients = new ArrayList<Client>();

        DatumReader<Client> reader = new SpecificDatumReader<>(Client.class);
        try (DataFileStream<Client> dataFileReader = new DataFileStream<>(new ByteArrayInputStream(avroFileContent), reader)) {
            while (dataFileReader.hasNext()) {

                //clients.add(dataFileReader.next());
            }
        } catch (IOException e) {
            var msg = String.format("Exception occurs during getting clients from avro file: %s/%s ", blobInfo.getBucket(), blobInfo.getName());
            LOG.error(String.format(msg, e));
            throw new AvroFileValidationException(msg);
        }
        return null;
    }

    public void saveToStorage(InputStream inputStream, BlobInfo blobInfo) {
        if (false/*Files.size(uploadFrom) > 1_000_000*/) {
            writeLargeFileToStorage(inputStream, blobInfo);
        } else {
            byte[] bytes = new byte[0];
            try {
                bytes = inputStream.readAllBytes();
            } catch (IOException e) {
                LOG.error(String.format("Exception occurred during file: %s/%s writing to storage", blobInfo.getBucket(), blobInfo.getName()), e);
            }
            // create the blob in one request.
            storage.create(blobInfo, bytes);
        }
        LOG.info("Blob was created: %s/%s", blobInfo.getBucket(), blobInfo.getName());
    }

    /**
     * When content is not available or large (1MB or more) it is recommended to write it in chunks via the blob's channel writer.
     *
     * @param inputStream
     * @param blobInfo
     */
    public void writeLargeFileToStorage(InputStream inputStream, BlobInfo blobInfo) {
        try (WriteChannel writer = storage.writer(blobInfo)) {
            byte[] buffer = new byte[1024];
            int limit;
            while ((limit = inputStream.read(buffer)) >= 0) {
                writer.write(ByteBuffer.wrap(buffer, 0, limit));
            }
        } catch (IOException e) {
            LOG.error(String.format("Exception occurred during file: %s/%s writing to storage", blobInfo.getBucket(), blobInfo.getName()), e);
        }

    }

    public InputStream readLargeFileFromStorage(BlobInfo blobInfo) {
        return null;
    }
}
