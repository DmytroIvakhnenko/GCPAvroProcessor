package io.github.dmytroivakhnenko.gcpavroprocessor.service;

import com.google.cloud.bigquery.Job;
import com.google.cloud.storage.BlobInfo;

import java.util.List;

public interface GCSFileProcessorService {

    List<Job> processFileToBigQuery(BlobInfo blobInfo);
}
