package io.github.dmytroivakhnenko.gcpavroprocessor.util;

import com.google.cloud.storage.BlobInfo;

import java.util.UUID;

public class CloudFileUtils {
    public static String constructGCSUri(BlobInfo blobInfo) {
        return String.format("gs://%s/%s", blobInfo.getBucket(), blobInfo.getName());
    }

    public static BlobInfo getTmpAvroFile(String bucket) {
        var tmpName = UUID.randomUUID() + "tmp_file" + AvroFileUtils.getAvroFileExt();
        return BlobInfo.newBuilder(bucket, tmpName).setContentType("application/avro").build();
    }

    public static BlobInfo getAvroFile(String bucket, String name) {
        return BlobInfo.newBuilder(bucket, name + AvroFileUtils.getAvroFileExt()).setContentType("application/avro").build();
    }
}
