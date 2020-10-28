package io.github.dmytroivakhnenko.gcpavroprocessor.service.impl;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.Job;
import com.google.cloud.storage.BlobInfo;
import example.gcp.Client;
import example.gcp.ClientMandatory;
import io.github.dmytroivakhnenko.gcpavroprocessor.exception.AvroFileGenerationException;
import io.github.dmytroivakhnenko.gcpavroprocessor.exception.AvroFileValidationException;
import io.github.dmytroivakhnenko.gcpavroprocessor.repository.BigQueryRepository;
import io.github.dmytroivakhnenko.gcpavroprocessor.repository.CloudStorageRepository;
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
import org.springframework.stereotype.Service;
import org.threeten.bp.Duration;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.dmytroivakhnenko.gcpavroprocessor.util.AvroFileUtils.createRandomClient;
import static io.github.dmytroivakhnenko.gcpavroprocessor.util.ClientUtils.createMandatoryClientFromClient;
import static io.github.dmytroivakhnenko.gcpavroprocessor.util.CloudFileUtils.*;

@Service
public class GCSFileProcessorServiceImpl implements GCSFileProcessorService {
    private static final Logger LOG = LoggerFactory.getLogger(GCSFileProcessorServiceImpl.class);
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final CloudStorageRepository gcStorage;
    private final BigQueryRepository bqRepository;

    @Value("${gcs.tmp.bucket.name}")
    private static String tmpBucketName;

    @Value("${gcs.input.bucket.name}")
    private static String inputBucketName;

    @Value("${gcs.generator.bucket.name}")
    private static String generatorBucketName;

    @Value("${bigquery.tableName.full}")
    private static String tableNameFull;

    @Value("${bigquery.tableName.mandatory}")
    private static String tableNameMandatory;

    @Value("${spring.cloud.gcp.bigquery.datasetName}")
    private static String datasetName;

    public GCSFileProcessorServiceImpl(CloudStorageRepository gcsFileProcessorService, BigQueryRepository bigQueryRepository) {
        this.gcStorage = gcsFileProcessorService;
        this.bqRepository = bigQueryRepository;
    }

    @Override
    public List<CompletableFuture<Boolean>> processFileToBigQuery(BlobInfo blobInfo) {
        LOG.info("File {} started processing", constructGCSUri(blobInfo));
        var mandatoryClientBlobInfo = validateAvroFileAndCreateFileWithMandatoryFields(blobInfo);
        return Stream.of(LoadInfo.of(blobInfo, tableNameFull), LoadInfo.of(mandatoryClientBlobInfo, tableNameMandatory, true))
                .map(this::loadAvroFileToBigQuery)
                .collect(Collectors.toList());
    }

    @Override
    public void generateRandomAvroFiles(String name, int fileCount, int clientsCount) {
        BlobInfo blobInfo;
        DatumWriter<Client> clientDatumWriter = new SpecificDatumWriter<>(Client.class);
        var client = new Client();
        for (int i = 0; i < fileCount; i++) {
            blobInfo = getAvroFile(generatorBucketName, name + i);
            try (var outputStream = gcStorage.createFileAndGetOutputStream(blobInfo);
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
            gcStorage.moveFileToBucket(blobInfo, inputBucketName);
        }
    }

    private CompletableFuture<Boolean> loadAvroFileToBigQuery(LoadInfo loadInfo) {
        var future = CompletableFuture.supplyAsync(() -> waitForJob(bqRepository.loadAvroFileToRepository(datasetName, loadInfo)), executorService);
        if (loadInfo.isTemporaryFile()) {
            future.thenRun(() -> gcStorage.deleteFile(loadInfo.getBlobInfo()));
        }
        return future;
    }

    public BlobInfo validateAvroFileAndCreateFileWithMandatoryFields(BlobInfo blobInfo) {
        var tmpBlob = getTmpAvroFile(tmpBucketName);
        var counter = 0;

        LOG.info("Validation of file {} started", constructGCSUri(blobInfo));
        DatumReader<Client> reader = new SpecificDatumReader<>(Client.class);
        DatumWriter<ClientMandatory> clientDatumWriter = new SpecificDatumWriter<>(ClientMandatory.class);

        try (var outputStream = gcStorage.createFileAndGetOutputStream(tmpBlob);
             var avroFileInputStream = gcStorage.getInputStreamForFile(blobInfo);
             DataFileStream<Client> clientDataFileReader = new DataFileStream<>(avroFileInputStream, reader);
             DataFileWriter<ClientMandatory> clientMandatoryDataFileWriter = new DataFileWriter<>(clientDatumWriter)) {
            clientMandatoryDataFileWriter.create(ClientMandatory.getClassSchema(), outputStream);
            LOG.info("Temporary file for mandatory info {} was created", constructGCSUri(tmpBlob));
            while (clientDataFileReader.hasNext()) {
                counter++;
                var client = clientDataFileReader.next();
                clientMandatoryDataFileWriter.append(createMandatoryClientFromClient(client));
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
}