package io.github.dmytroivakhnenko.gcpavroprocessor.service;

import com.google.cloud.bigquery.Job;
import com.google.cloud.storage.BlobInfo;
import com.google.common.util.concurrent.ListenableFuture;

public interface GCSFileProcessorService {
    void processFile(BlobInfo blobInfo);

    ListenableFuture<Job> processFileViaIntegration(BlobInfo blobInfo);
}
