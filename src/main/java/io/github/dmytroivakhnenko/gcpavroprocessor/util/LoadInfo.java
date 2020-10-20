package io.github.dmytroivakhnenko.gcpavroprocessor.util;

import com.google.cloud.storage.BlobInfo;

public class LoadInfo {
    private final BlobInfo blobInfo;
    private final String tableName;
    private final boolean isTemporaryFile;

    private LoadInfo(BlobInfo blobInfo, String tableName, boolean isTemporaryFile) {
        this.blobInfo = blobInfo;
        this.tableName = tableName;
        this.isTemporaryFile = isTemporaryFile;
    }

    public static LoadInfo of(BlobInfo blobInfo, String tableName) {
        return of(blobInfo, tableName, false);
    }

    public static LoadInfo of(BlobInfo blobInfo, String tableName, boolean isTemporaryFile) {
        return new LoadInfo(blobInfo, tableName, isTemporaryFile);
    }

    public BlobInfo getBlobInfo() {
        return blobInfo;
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isTemporaryFile() {
        return isTemporaryFile;
    }
}
