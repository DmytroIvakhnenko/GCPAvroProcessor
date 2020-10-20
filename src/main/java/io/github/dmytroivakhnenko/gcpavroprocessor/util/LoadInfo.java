package io.github.dmytroivakhnenko.gcpavroprocessor.util;

import com.google.cloud.storage.BlobInfo;

public class LoadInfo {
    private final BlobInfo blobInfo;
    private final String tableName;

    private LoadInfo(BlobInfo blobInfo, String tableName) {
        this.blobInfo = blobInfo;
        this.tableName = tableName;
    }

    public static LoadInfo of(BlobInfo blobInfo, String tableName) {
        return new LoadInfo(blobInfo, tableName);
    }

    public BlobInfo getBlobInfo() {
        return blobInfo;
    }

    public String getTableName() {
        return tableName;
    }
}
