package io.github.dmytroivakhnenko.gcpavroprocessor.controller;

import com.google.cloud.RetryOption;
import com.google.cloud.bigquery.Job;
import com.google.cloud.storage.BlobInfo;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.github.dmytroivakhnenko.gcpavroprocessor.service.GCSFileProcessorService;
import io.github.dmytroivakhnenko.gcpavroprocessor.util.PubSubEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.threeten.bp.Duration;

import java.util.Base64;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * PubsubController consumes a Pub/Sub message (JSON format)
 */
@RestController
public class PubSubController {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubController.class);

    @Value("${avro.file.ext}")
    private String avroFileExt;

    private GCSFileProcessorService gcsFileProcessorService;

    public PubSubController(
            GCSFileProcessorService gcsFileProcessorService) {
        this.gcsFileProcessorService = gcsFileProcessorService;
    }

    @PostMapping("/pubsub")
    public ResponseEntity receiveMessage(@RequestBody PubSubEvent event) {
        // Get PubSub message from request body.
        var payload = Optional.ofNullable(event.getMessage());
        if (payload.isEmpty()) {
            var msg = "Bad Request: invalid Pub/Sub message format";
            LOG.error(msg);
            return new ResponseEntity(msg, HttpStatus.BAD_REQUEST);
        }

        // Decode the Pub/Sub message.
        var payloadData = payload.get().getData();
        JsonObject data;
        try {
            var decodedMessage = new String(Base64.getDecoder().decode(payloadData));
            LOG.info("decodedMessage: " + decodedMessage);
            Gson gson = new Gson();
            data = gson.fromJson(decodedMessage, JsonObject.class);
            LOG.info(data.toString());
        } catch (Exception e) {
            var msg = "Error: Invalid Pub/Sub message: data property is not valid base64 encoded JSON";
            LOG.error(msg, e);
            return new ResponseEntity(msg, HttpStatus.BAD_REQUEST);
        }

        var fileName = Optional.ofNullable(data.get("name"));
        var bucketName = Optional.ofNullable(data.get("bucket"));
        // Validate the message is a Cloud Storage event.
        if (fileName.isEmpty() || bucketName.isEmpty()) {
            var msg = "Error: Invalid Cloud Storage notification: expected name and bucket properties";
            LOG.error(msg);
            return new ResponseEntity(msg, HttpStatus.BAD_REQUEST);
        }

        if (!fileName.get().getAsString().endsWith(avroFileExt)) {
            LOG.info("File " + fileName.get().getAsString() + " was skipped from processing due to the wrong extension");
            return new ResponseEntity(HttpStatus.OK);
        }

        LOG.info("Name = " + data.get("name") + " Bucket = " + data.get("bucket"));

        var blobInfo = BlobInfo.newBuilder(bucketName.get().getAsString(), fileName.get().getAsString()).build();

        return getResponse(gcsFileProcessorService.processFileViaIntegration(blobInfo));
    }

    private ResponseEntity getResponse(CompletableFuture<Job> loadJob) {
        try {
            Job job = loadJob.get(10, TimeUnit.MINUTES);
            job.waitFor(RetryOption.totalTimeout(Duration.ofMinutes(10)));
            return new ResponseEntity(HttpStatus.OK);
        } catch (Exception e) {
            var msg = "Error during data load to BigQuery";
            LOG.error(msg, e);
            return new ResponseEntity(msg, HttpStatus.BAD_REQUEST);
        }
    }
}