package io.github.dmytroivakhnenko.gcpavroprocessor.service;

import com.google.cloud.storage.BlobInfo;

public interface GCSFileProcessorService {
    void processFile(BlobInfo blobInfo);
}
