package io.github.dmytroivakhnenko.gcpavroprocessor.repository;

import com.google.cloud.bigquery.Job;
import io.github.dmytroivakhnenko.gcpavroprocessor.util.LoadInfo;

public interface BigQueryRepository {
    Job loadAvroFileToDataset(String dataset, LoadInfo loadInfo);

    Boolean waitForJob(Job job);
}
