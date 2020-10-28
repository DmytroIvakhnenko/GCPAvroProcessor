package io.github.dmytroivakhnenko.gcpavroprocessor.repository;

import com.google.cloud.storage.BlobInfo;

import java.io.InputStream;
import java.io.OutputStream;

public interface CloudStorageRepository {
    InputStream getInputStreamForFile(BlobInfo blobInfo);

    OutputStream createFileAndGetOutputStream(BlobInfo blobInfo);

    void deleteFile(BlobInfo blobInfo);

    void moveFileToBucket(BlobInfo blobInfo, String targetBucket);
}
