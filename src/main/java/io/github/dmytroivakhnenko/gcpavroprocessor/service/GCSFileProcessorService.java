package io.github.dmytroivakhnenko.gcpavroprocessor.service;

import com.google.cloud.bigquery.Job;
import com.google.cloud.storage.BlobInfo;

import java.util.concurrent.CompletableFuture;

public interface GCSFileProcessorService {
    /*void processFile(BlobInfo blobInfo);*/

    CompletableFuture<Job> processFileViaIntegration(BlobInfo blobInfo);
}
