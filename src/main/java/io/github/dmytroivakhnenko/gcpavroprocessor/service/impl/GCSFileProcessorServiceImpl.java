package io.github.dmytroivakhnenko.gcpavroprocessor.service.impl;

import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import example.gcp.Client;
import example.gcp.ClientMandatory;
import io.github.dmytroivakhnenko.gcpavroprocessor.exception.AvroFileValidationException;
import io.github.dmytroivakhnenko.gcpavroprocessor.service.GCSFileProcessorService;
import io.github.dmytroivakhnenko.gcpavroprocessor.util.LoadInfo;
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class GCSFileProcessorServiceImpl implements GCSFileProcessorService {
    private static final Logger LOG = LoggerFactory.getLogger(GCSFileProcessorServiceImpl.class);
    private static final Storage storage = StorageOptions.getDefaultInstance().getService();
    private static final BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    @Value("${bigquery.tableName.full}")
    private String tableNameFull;

    @Value("${bigquery.tableName.mandatory}")
    private String tableNameMandatory;

    @Value("${gcs.tmp.bucket.name}")
    private String tmpBucketName;

    @Value("${spring.cloud.gcp.bigquery.datasetName}")
    private String datasetName;

    @Override
    public List<Job> processFileToBigQuery(BlobInfo blobInfo) {
        var blob = storage.get(BlobId.of(blobInfo.getBucket(), blobInfo.getName()));

        LOG.info(String.format("File %s/%s received", blobInfo.getBucket(), blobInfo.getName()));
        var mandatoryClientBlobInfo = validateAvroFileAndGetMandatoryClientBlobInfo(blobInfo);
        //return bigQueryFileGateway.writeToBigQueryTable(blob.getContent(), tableNameFull);
        return Arrays.asList(LoadInfo.of(blobInfo, tableNameFull), LoadInfo.of(mandatoryClientBlobInfo, tableNameMandatory)).parallelStream().map(this::loadAvroFileToBigQuery).collect(Collectors.toList());
    }

    private Job loadAvroFileToBigQuery(LoadInfo loadInfo) {
        var blobInfo = loadInfo.getBlobInfo();
        TableId tableId = TableId.of(datasetName, loadInfo.getTableName());
        LoadJobConfiguration loadConfig = LoadJobConfiguration.of(tableId, constructGCSUri(blobInfo), FormatOptions.avro());
        // Load data from a GCS Avro file into the table
        var job = bigquery.create(JobInfo.of(loadConfig));
        LOG.info("Job: {} processing file {}/{} was started", job.getJobId(), blobInfo.getBucket(), blobInfo.getName());
        return job;
    }

    public BlobInfo validateAvroFileAndGetMandatoryClientBlobInfo(BlobInfo blobInfo) {
        var tmpName = UUID.randomUUID() + "tmp_file.avro";
        var tmpBlob = BlobInfo.newBuilder(tmpBucketName, tmpName).setContentType("application/avro").build();
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
        return tmpBlob;
    }

    private ClientMandatory createMandatoryClient(Client client) {
        var clientMandatory = new ClientMandatory();
        clientMandatory.setId(client.getId());
        clientMandatory.setName(client.getName());
        return clientMandatory;
    }

    public OutputStream getStorageOutputStream(BlobInfo blobInfo) {
        var storageResource = new GoogleStorageResource(storage, constructGCSUri(blobInfo));
        var blob = storageResource.createBlob();
        WriteChannel writer = blob.writer();
        writer.setChunkSize(64 * 1024);
        return Channels.newOutputStream(writer);
    }

    public InputStream readFileFromStorage(BlobInfo blobInfo) {
        var blob = storage.get(blobInfo.getBlobId());
        var reader = blob.reader();
        reader.setChunkSize(64 * 1024);
        return Channels.newInputStream(reader);
    }

    private String constructGCSUri(BlobInfo blobInfo) {
        return String.format("gs://%s/%s", blobInfo.getBucket(), blobInfo.getName());
    }
}