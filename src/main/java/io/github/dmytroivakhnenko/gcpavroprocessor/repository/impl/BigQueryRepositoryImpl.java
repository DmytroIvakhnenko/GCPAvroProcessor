package io.github.dmytroivakhnenko.gcpavroprocessor.repository.impl;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.*;
import io.github.dmytroivakhnenko.gcpavroprocessor.repository.BigQueryRepository;
import io.github.dmytroivakhnenko.gcpavroprocessor.util.LoadInfo;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import org.threeten.bp.Duration;

import java.util.Objects;

import static io.github.dmytroivakhnenko.gcpavroprocessor.util.CloudFileUtils.constructGCSUri;

@Repository
@Slf4j
public class BigQueryRepositoryImpl implements BigQueryRepository {
    private static final BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
    private static final int JOB_TOTAL_TIMEOUT_MINUTES = 5;
    private static final int JOB_RETRY_COUNT = 5;

    @Override
    public Job loadAvroFileToDataset(String dataset, LoadInfo loadInfo) {
        var blobInfo = loadInfo.getBlobInfo();
        var tableId = TableId.of(dataset, loadInfo.getTableName());
        var loadConfig = LoadJobConfiguration.of(tableId, constructGCSUri(blobInfo), FormatOptions.avro());
        // Load data from a GCS Avro file into the table
        var job = bigquery.create(JobInfo.of(loadConfig));
        log.info("Job: {} processing file {} was started", job.getJobId(), constructGCSUri(blobInfo));
        return job;
    }

    @Override
    public Boolean waitForJob(Job job) {
        try {
            log.info("Waiting for job {} to finish ...", job.getJobId());
            job.waitFor(RetryOption.totalTimeout(Duration.ofMinutes(JOB_TOTAL_TIMEOUT_MINUTES)), RetryOption.maxAttempts(JOB_RETRY_COUNT));
            if (job.isDone()) {
                log.info("Job {} was finished", job.getJobId());
                if (!Objects.isNull(job.getStatus().getError())) {
                    log.error("Job {} was finished with error {}", job.getJobId(), job.getStatus().getError());
                    return false;
                } else {
                    log.info("Job {} was finished without any errors", job.getJobId());
                    return true;
                }
            } else {
                log.error("Job {} wasn't finished", job.getJobId());
                return false;
            }
        } catch (InterruptedException e) {
            log.error("Exception occurred during job {} execution", job.getJobId(), e);
            return false;
        }
    }
}
