package io.github.dmytroivakhnenko.gcpavroprocessor.repository.impl;

import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.github.dmytroivakhnenko.gcpavroprocessor.repository.CloudStorageRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cloud.gcp.storage.GoogleStorageResource;
import org.springframework.stereotype.Repository;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;

import static io.github.dmytroivakhnenko.gcpavroprocessor.util.CloudFileUtils.constructGCSUri;

@Repository
@Slf4j
public class CloudStorageRepositoryImpl implements CloudStorageRepository {
    private static final Storage storage = StorageOptions.getDefaultInstance().getService();
    private static final int CHUNK_SIZE = 2 * 1024 * 1024;

    @Override
    public InputStream getInputStreamForFile(BlobInfo blobInfo) {
        var blob = storage.get(blobInfo.getBlobId());
        var reader = blob.reader();
        reader.setChunkSize(CHUNK_SIZE);
        return Channels.newInputStream(reader);
    }

    @Override
    public OutputStream createFileAndGetOutputStream(BlobInfo blobInfo) {
        var storageResource = new GoogleStorageResource(storage, constructGCSUri(blobInfo));
        var blob = storageResource.createBlob();
        var writer = blob.writer();
        writer.setChunkSize(CHUNK_SIZE);
        return Channels.newOutputStream(writer);
    }

    @Override
    public void deleteFile(BlobInfo blobInfo) {
        if (storage.delete(blobInfo.getBlobId())) {
            log.info("Temp file {} was deleted", constructGCSUri(blobInfo));
        } else {
            log.error("Temp file {} wasn't deleted", constructGCSUri(blobInfo));
        }
    }

    @Override
    public void moveFileToBucket(BlobInfo blobInfo, String targetBucket) {
        var blob = storage.get(blobInfo.getBucket(), blobInfo.getName());
        var copyWriter = blob.copyTo(targetBucket, blobInfo.getName());
        copyWriter.getResult();
        blob.delete();
    }
}
