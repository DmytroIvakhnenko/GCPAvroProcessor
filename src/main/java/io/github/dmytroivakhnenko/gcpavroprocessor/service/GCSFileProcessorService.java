package io.github.dmytroivakhnenko.gcpavroprocessor.service;

import com.google.cloud.bigquery.Job;
import com.google.cloud.storage.BlobInfo;
import org.springframework.util.concurrent.ListenableFuture;

public interface GCSFileProcessorService {
    void processFile(BlobInfo blobInfo);

    ListenableFuture<Job> processFileViaIntegration(BlobInfo blobInfo);
}
