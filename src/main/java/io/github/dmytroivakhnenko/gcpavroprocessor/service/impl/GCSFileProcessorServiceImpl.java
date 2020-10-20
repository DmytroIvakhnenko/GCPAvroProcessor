package io.github.dmytroivakhnenko.gcpavroprocessor.service.impl;

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import example.gcp.Client;
import example.gcp.ClientMandatory;
import io.github.dmytroivakhnenko.gcpavroprocessor.config.BigQueryIntegrationConfig;
import io.github.dmytroivakhnenko.gcpavroprocessor.exception.AvroFileValidationException;
import io.github.dmytroivakhnenko.gcpavroprocessor.service.GCSFileProcessorService;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gcp.storage.GoogleStorageResource;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

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
    public CompletableFuture<Job> processFileViaIntegration(BlobInfo blobInfo) {
        var blob = storage.get(BlobId.of(blobInfo.getBucket(), blobInfo.getName()));

        LOG.info(String.format("File %s/%s received", blobInfo.getBucket(), blobInfo.getName()));
        getClientsStreamFromAvroFile(blobInfo);
        //return bigQueryFileGateway.writeToBigQueryTable(blob.getContent(), tableNameFull);

        return loadAvroFromGcs(constructGCSUri(blobInfo));
    }

    private CompletableFuture<Job> loadAvroFromGcs(String sourceUri) {
        var datasetName = "clients_dataset";
        var tableName = "client_full";
        // Initialize client that will be used to send requests. This client only needs to be created
        // once, and can be reused for multiple requests.
        BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

        TableId tableId = TableId.of(datasetName, tableName);
        LoadJobConfiguration loadConfig = LoadJobConfiguration.of(tableId, sourceUri, FormatOptions.avro());

        // Load data from a GCS Avro file into the table
        CompletableFuture<Job> job = CompletableFuture.supplyAsync(() -> bigquery.create(JobInfo.of(loadConfig)));
        return job;
    }

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

    public void getClientsStreamFromAvroFile(BlobInfo blobInfo) {
        var tmpName = UUID.randomUUID() + "tmp_file.avro";
        var tmpBlob = BlobInfo.newBuilder(blobInfo.getBucket(), tmpName).setContentType("application/avro").build();
        var outputStream = getStorageOutputStream(tmpBlob);
        var avroFileInputStream = readFileFromStorage(blobInfo);
        var count = 0;

        DatumReader<Client> reader = new SpecificDatumReader<>(Client.class);
        DatumWriter<ClientMandatory> clientDatumWriter = new SpecificDatumWriter<>(ClientMandatory.class);

        try (DataFileStream<Client> clientDataFileReader = new DataFileStream<>(avroFileInputStream, reader);
             DataFileWriter<ClientMandatory> clientMandatoryDataFileWriter = new DataFileWriter<>(clientDatumWriter)) {
            clientMandatoryDataFileWriter.create(ClientMandatory.getClassSchema(), outputStream);
            while (clientDataFileReader.hasNext()) {
                var client = clientDataFileReader.next();
                clientMandatoryDataFileWriter.append(createMandatoryClient(client));
                count++;
            }
        } catch (IOException e) {
            var msg = String.format("Exception occurs during getting clients from avro file: %s/%s ", blobInfo.getBucket(), blobInfo.getName());
            LOG.error(String.format(msg, e));
            throw new AvroFileValidationException(msg);
        }
        LOG.info("Clients count: " + count);
    }

    private ClientMandatory createMandatoryClient(Client client) {
        var clientMandatory = new ClientMandatory();
        clientMandatory.setId(client.getId());
        clientMandatory.setName(client.getName());
        return clientMandatory;
    }

    public OutputStream getStorageOutputStream(BlobInfo blobInfo) {
        GoogleStorageResource storageResource = new GoogleStorageResource(storage, constructGCSUri(blobInfo));
        var blob = storageResource.createBlob();
        WriteChannel writer = blob.writer();
        writer.setChunkSize(64 * 1024);
        return Channels.newOutputStream(writer);
    }

    public InputStream readFileFromStorage(BlobInfo blobInfo) {
        var blob = storage.get(blobInfo.getBlobId());
        ReadChannel reader = blob.reader();
        reader.setChunkSize(64 * 1024);
        return Channels.newInputStream(reader);
    }

    private String constructGCSUri(BlobInfo blobInfo) {
        return String.format("gs://%s/%s", blobInfo.getBucket(), blobInfo.getName());
    }
}