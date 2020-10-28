package io.github.dmytroivakhnenko.gcpavroprocessor.repository;

import com.google.cloud.bigquery.Job;
import io.github.dmytroivakhnenko.gcpavroprocessor.util.LoadInfo;

public interface BigQueryRepository {
    Job loadAvroFileToRepository(String dataset, LoadInfo loadInfo);
}
