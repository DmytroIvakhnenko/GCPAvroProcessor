package io.github.dmytroivakhnenko.gcpavroprocessor.service.impl;

import com.google.cloud.storage.BlobInfo;
import example.gcp.Client;
import example.gcp.ClientMandatory;
import io.github.dmytroivakhnenko.gcpavroprocessor.exception.AvroFileGenerationException;
import io.github.dmytroivakhnenko.gcpavroprocessor.exception.AvroFileValidationException;
import io.github.dmytroivakhnenko.gcpavroprocessor.repository.BigQueryRepository;
import io.github.dmytroivakhnenko.gcpavroprocessor.repository.CloudStorageRepository;
import io.github.dmytroivakhnenko.gcpavroprocessor.service.GCSFileProcessorService;
import io.github.dmytroivakhnenko.gcpavroprocessor.util.LoadInfo;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

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
@Slf4j
@RequiredArgsConstructor
public class GCSFileProcessorServiceImpl implements GCSFileProcessorService {
    private final ExecutorService executorService = Executors.newCachedThreadPool();
    private final CloudStorageRepository gcStorage;
    private final BigQueryRepository bqRepository;

    @Value("${gcs.tmp.bucket.name}")
    private String tmpBucketName;

    @Value("${gcs.input.bucket.name}")
    private String inputBucketName;

    @Value("${gcs.generator.bucket.name}")
    private String generatorBucketName;

    @Value("${bigquery.tableName.full}")
    private String tableNameFull;

    @Value("${bigquery.tableName.mandatory}")
    private String tableNameMandatory;

    @Value("${spring.cloud.gcp.bigquery.datasetName}")
    private String datasetName;

    @Override
    public List<CompletableFuture<Boolean>> processFileToBigQuery(BlobInfo blobInfo) {
        log.info("File {} started processing", constructGCSUri(blobInfo));
        var mandatoryClientBlobInfo = validateAvroFileAndCreateFileWithMandatoryFields(blobInfo);
        return Stream.of(LoadInfo.builder().blobInfo(blobInfo).tableName(tableNameFull).build(), LoadInfo.builder().blobInfo(mandatoryClientBlobInfo).tableName(tableNameMandatory).temporaryFile(true).build())
                .map(this::loadAvroFileToBigQuery)
                .collect(Collectors.toList());
    }

    @Override
    public void generateRandomAvroFiles(String name, int fileCount, int clientsCount) {
        BlobInfo blobInfo;
        var clientDatumWriter = new SpecificDatumWriter<>(Client.class);
        var client = new Client();
        for (int i = 0; i < fileCount; i++) {
            blobInfo = getAvroFile(generatorBucketName, name + i);
            try (var outputStream = gcStorage.createFileAndGetOutputStream(blobInfo);
                 var clientDataFileWriter = new DataFileWriter<>(clientDatumWriter)) {
                clientDataFileWriter.create(Client.getClassSchema(), outputStream);
                for (int j = 0; j < clientsCount; j++) {
                    client = createRandomClient();
                    clientDataFileWriter.append(client);
                }
            } catch (IOException e) {
                var msg = String.format("Exception occurs during generating clients for avro file: %s ", constructGCSUri(blobInfo));
                log.error(msg, e);
                throw new AvroFileGenerationException(msg);
            }
            gcStorage.moveFileToBucket(blobInfo, inputBucketName);
        }
    }

    private CompletableFuture<Boolean> loadAvroFileToBigQuery(LoadInfo loadInfo) {
        var future = CompletableFuture.supplyAsync(() -> bqRepository.waitForJob(bqRepository.loadAvroFileToDataset(datasetName, loadInfo)), executorService);
        if (loadInfo.isTemporaryFile()) {
            future.thenRun(() -> gcStorage.deleteFile(loadInfo.getBlobInfo()));
        }
        return future;
    }

    public BlobInfo validateAvroFileAndCreateFileWithMandatoryFields(BlobInfo blobInfo) {
        var tmpBlob = getTmpAvroFile(tmpBucketName);
        var counter = 0;

        log.info("Validation of file {} started", constructGCSUri(blobInfo));
        var clientDatumReader = new SpecificDatumReader<>(Client.class);
        var mandatoryClientDatumWriter = new SpecificDatumWriter<>(ClientMandatory.class);

        try (var outputStream = gcStorage.createFileAndGetOutputStream(tmpBlob);
             var inputStream = gcStorage.getInputStreamForFile(blobInfo);
             var clientDataFileReader = new DataFileStream<>(inputStream, clientDatumReader);
             var mandatoryClientDataFileWriter = new DataFileWriter<>(mandatoryClientDatumWriter)) {
            mandatoryClientDataFileWriter.create(ClientMandatory.getClassSchema(), outputStream);
            log.info("Temporary file for mandatory info {} was created", constructGCSUri(tmpBlob));
            while (clientDataFileReader.hasNext()) {
                counter++;
                var client = clientDataFileReader.next();
                mandatoryClientDataFileWriter.append(createMandatoryClientFromClient(client));
            }
        } catch (IOException e) {
            var msg = String.format("Exception occurs during getting clients from avro file: %s ", constructGCSUri(blobInfo));
            log.error(msg, e);
            throw new AvroFileValidationException(msg);
        }

        log.info("Number of processed Clients: {}", counter);
        log.info("Validation of file {} was successfully finished, temporary file for mandatory info {} was successfully loaded", constructGCSUri(blobInfo), constructGCSUri(tmpBlob));
        return tmpBlob;
    }
}