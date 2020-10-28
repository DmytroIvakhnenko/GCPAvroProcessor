package io.github.dmytroivakhnenko.gcpavroprocessor.util;

import com.google.cloud.storage.BlobInfo;
import lombok.Builder;
import lombok.Getter;

@Builder
@Getter
public class LoadInfo {
    private final BlobInfo blobInfo;
    private final String tableName;
    private final boolean temporaryFile;
}

