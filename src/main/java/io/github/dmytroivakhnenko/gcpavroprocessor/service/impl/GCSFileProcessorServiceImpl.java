package io.github.dmytroivakhnenko.gcpavroprocessor.service.impl;

import com.google.cloud.RetryOption;
import com.google.cloud.WriteChannel;
import com.google.cloud.bigquery.*;
import com.google.cloud.storage.*;
import example.gcp.Client;
import example.gcp.ClientMandatory;
import io.github.dmytroivakhnenko.gcpavroprocessor.exception.AvroFileGenerationException;
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
import org.threeten.bp.Duration;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.dmytroivakhnenko.gcpavroprocessor.util.AvroFileGenerator.AVRO_FILE_EXT;
import static io.github.dmytroivakhnenko.gcpavroprocessor.util.AvroFileGenerator.createRandomClient;

@Service
public class GCSFileProcessorServiceImpl implements GCSFileProcessorService {
    private static final Logger LOG = LoggerFactory.getLogger(GCSFileProcessorServiceImpl.class);

    private static final int CHUNK_SIZE = 2 * 1024 * 1024;
    private static final Storage storage = StorageOptions.getDefaultInstance().getService();
    private static final BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    private static final ExecutorService executorService = Executors.newCachedThreadPool();

    @Value("${bigquery.tableName.full}")
    private String tableNameFull;

    @Value("${bigquery.tableName.mandatory}")
    private String tableNameMandatory;

    @Value("${gcs.tmp.bucket.name}")
    private String tmpBucketName;

    @Value("${spring.cloud.gcp.bigquery.datasetName}")
    private String datasetName;


    @Override
    public void generateRandomAvroFiles(String name, int fileCount, int clientsCount) {
        final String sourceBucketName = "gcp_avro_processor_generator_bucket";
        final String targetBucketName = "gcp_avro_processor_input_bucket";

        List<BlobInfo> filesList = new ArrayList<>();
        BlobInfo blobInfo;
        DatumWriter<Client> clientDatumWriter = new SpecificDatumWriter<>(Client.class);
        var client = new Client();
        for (int i = 0; i < fileCount; i++) {
            blobInfo = BlobInfo.newBuilder(sourceBucketName, name + i + AVRO_FILE_EXT).setContentType("application/avro").build();
            filesList.add(blobInfo);
            try (var outputStream = createOutputStreamForFile(blobInfo);
                 DataFileWriter<Client> clientDataFileWriter = new DataFileWriter<>(clientDatumWriter)) {
                clientDataFileWriter.create(Client.getClassSchema(), outputStream);
                for (int j = 0; j < clientsCount; j++) {
                    client = createRandomClient();
                    clientDataFileWriter.append(client);
                }
            } catch (IOException e) {
                var msg = String.format("Exception occurs during generating clients for avro file: %s ", constructGCSUri(blobInfo));
                LOG.error(msg, e);
                throw new AvroFileGenerationException(msg);
            }
        }
        Blob blob;
        Blob copiedBlob;
        CopyWriter copyWriter;
        for (BlobInfo b : filesList) {
            blob = storage.get(sourceBucketName, b.getName());
            copyWriter = blob.copyTo(targetBucketName, b.getName());
            copiedBlob = copyWriter.getResult();
            blob.delete();
        }
    }

    @Override
    public List<CompletableFuture<Boolean>> processFileToBigQuery(BlobInfo blobInfo) {
        var blob = storage.get(BlobId.of(blobInfo.getBucket(), blobInfo.getName()));
        LOG.info("File {} started processing", constructGCSUri(blobInfo));
        var mandatoryClientBlobInfo = validateAvroFileAndCreateFileWithMandatoryFields(blobInfo);
        return Stream.of(LoadInfo.of(blobInfo, tableNameFull), LoadInfo.of(mandatoryClientBlobInfo, tableNameMandatory, true))
                .map(this::loadAvroFileToBigQuery)
                .collect(Collectors.toList());
    }

    private CompletableFuture<Boolean> loadAvroFileToBigQuery(LoadInfo loadInfo) {
        var future = CompletableFuture.supplyAsync(() -> {
            var blobInfo = loadInfo.getBlobInfo();
            TableId tableId = TableId.of(datasetName, loadInfo.getTableName());
            LoadJobConfiguration loadConfig = LoadJobConfiguration.of(tableId, constructGCSUri(blobInfo), FormatOptions.avro());
            // Load data from a GCS Avro file into the table
            var job = bigquery.create(JobInfo.of(loadConfig));
            LOG.info("Job: {} processing file {} was started", job.getJobId(), constructGCSUri(blobInfo));
            return waitForJob(job);
        }, executorService);
        if (loadInfo.isTemporaryFile()) {
            future.thenRun(() -> deleteFileFromStorage(loadInfo.getBlobInfo()));
        }
        return future;
    }

    private Boolean waitForJob(Job job) {
        try {
            LOG.info("Waiting for job {} to finish ...", job.getJobId());
            job.waitFor(RetryOption.totalTimeout(Duration.ofMinutes(10)));
            if (job.isDone()) {
                LOG.info("Job {} was successfully done", job.getJobId());
                return true;
            } else {
                LOG.error("Job {} was finished with error {}", job.getJobId(), job.getStatus().getError());
                return false;
            }
        } catch (InterruptedException e) {
            LOG.error("Exception occurred during job {} execution", job.getJobId(), e);
            return false;
        }
    }

    public BlobInfo validateAvroFileAndCreateFileWithMandatoryFields(BlobInfo blobInfo) {
        var tmpName = UUID.randomUUID() + "tmp_file.avro";
        var tmpBlob = BlobInfo.newBuilder(tmpBucketName, tmpName).setContentType("application/avro").build();
        var counter = 0;

        LOG.info("Validation of file {} started, temporary file for mandatory info {} was created", constructGCSUri(blobInfo), constructGCSUri(tmpBlob));
        DatumReader<Client> reader = new SpecificDatumReader<>(Client.class);
        DatumWriter<ClientMandatory> clientDatumWriter = new SpecificDatumWriter<>(ClientMandatory.class);

        try (var outputStream = createOutputStreamForFile(tmpBlob);
             var avroFileInputStream = createInputStreamForFile(blobInfo);
             DataFileStream<Client> clientDataFileReader = new DataFileStream<>(avroFileInputStream, reader);
             DataFileWriter<ClientMandatory> clientMandatoryDataFileWriter = new DataFileWriter<>(clientDatumWriter)) {
            clientMandatoryDataFileWriter.create(ClientMandatory.getClassSchema(), outputStream);
            while (clientDataFileReader.hasNext()) {
                counter++;
                var client = clientDataFileReader.next();
                clientMandatoryDataFileWriter.append(createMandatoryClient(client));
            }
        } catch (IOException e) {
            var msg = String.format("Exception occurs during getting clients from avro file: %s ", constructGCSUri(blobInfo));
            LOG.error(msg, e);
            throw new AvroFileValidationException(msg);
        }

        LOG.info("Number of processed Clients: {}", counter);
        LOG.info("Validation of file {} was successfully finished, temporary file for mandatory info {} was successfully loaded", constructGCSUri(blobInfo), constructGCSUri(tmpBlob));
        return tmpBlob;
    }

    private ClientMandatory createMandatoryClient(Client client) {
        return new ClientMandatory(client.getId(), client.getName());
    }

    public OutputStream createOutputStreamForFile(BlobInfo blobInfo) {
        var storageResource = new GoogleStorageResource(storage, constructGCSUri(blobInfo));
        var blob = storageResource.createBlob();
        WriteChannel writer = blob.writer();
        writer.setChunkSize(CHUNK_SIZE);
        return Channels.newOutputStream(writer);
    }

    public InputStream createInputStreamForFile(BlobInfo blobInfo) {
        var blob = storage.get(blobInfo.getBlobId());
        var reader = blob.reader();
        reader.setChunkSize(CHUNK_SIZE);
        return Channels.newInputStream(reader);
    }

    private void deleteFileFromStorage(BlobInfo blobInfo) {
        if (storage.delete(blobInfo.getBlobId())) {
            LOG.info("Temp file {} was deleted", constructGCSUri(blobInfo));
        } else {
            LOG.error("Temp file {} wasn't deleted", constructGCSUri(blobInfo));
        }
    }

    private String constructGCSUri(BlobInfo blobInfo) {
        return String.format("gs://%s/%s", blobInfo.getBucket(), blobInfo.getName());
    }
}