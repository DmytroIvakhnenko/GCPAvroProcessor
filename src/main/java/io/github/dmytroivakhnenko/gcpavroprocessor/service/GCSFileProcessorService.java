package io.github.dmytroivakhnenko.gcpavroprocessor.service;

import com.google.cloud.storage.BlobInfo;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface GCSFileProcessorService {
    List<CompletableFuture<Boolean>> processFileToBigQuery(BlobInfo blobInfo);

    void generateRandomAvroFiles(String name, int fileCount, int clientsCount);
}
