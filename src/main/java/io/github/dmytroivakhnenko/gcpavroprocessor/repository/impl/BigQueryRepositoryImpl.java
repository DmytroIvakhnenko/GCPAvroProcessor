package io.github.dmytroivakhnenko.gcpavroprocessor.repository.impl;

import com.google.cloud.bigquery.*;
import io.github.dmytroivakhnenko.gcpavroprocessor.repository.BigQueryRepository;
import io.github.dmytroivakhnenko.gcpavroprocessor.util.LoadInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import static io.github.dmytroivakhnenko.gcpavroprocessor.util.CloudFileUtils.constructGCSUri;

@Repository
public class BigQueryRepositoryImpl implements BigQueryRepository {
    private static final Logger LOG = LoggerFactory.getLogger(BigQueryRepositoryImpl.class);
    private static final BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

    @Override
    public Job loadAvroFileToRepository(String dataset, LoadInfo loadInfo) {
        var blobInfo = loadInfo.getBlobInfo();
        TableId tableId = TableId.of(dataset, loadInfo.getTableName());
        LoadJobConfiguration loadConfig = LoadJobConfiguration.of(tableId, constructGCSUri(blobInfo), FormatOptions.avro());
        // Load data from a GCS Avro file into the table
        var job = bigquery.create(JobInfo.of(loadConfig));
        LOG.info("Job: {} processing file {} was started", job.getJobId(), constructGCSUri(blobInfo));
        return job;
    }
}
