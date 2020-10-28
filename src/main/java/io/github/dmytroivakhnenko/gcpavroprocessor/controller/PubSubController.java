package io.github.dmytroivakhnenko.gcpavroprocessor.controller;

import com.google.cloud.storage.BlobInfo;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.github.dmytroivakhnenko.gcpavroprocessor.service.GCSFileProcessorService;
import io.github.dmytroivakhnenko.gcpavroprocessor.util.PubSubEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.Base64;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.github.dmytroivakhnenko.gcpavroprocessor.util.AvroFileUtils.AVRO_FILE_EXT;

/**
 * PubsubController consumes a Pub/Sub message (JSON format)
 */
@RestController
@RequiredArgsConstructor
@Slf4j
public class PubSubController {
    private final GCSFileProcessorService gcsFileProcessorService;

    @PostMapping("/pubsub")
    public ResponseEntity receiveMessage(@RequestBody PubSubEvent event) {
        // Get PubSub message from request body
        var payload = Optional.ofNullable(event.getMessage());
        if (payload.isEmpty()) {
            return logAndReturnBadRequest("Invalid Pub/Sub message format");
        }

        // Decode the Pub/Sub message
        var payloadData = payload.get().getData();
        JsonObject data;
        try {
            var decodedMessage = new String(Base64.getDecoder().decode(payloadData));
            log.info("PubSub message received {}", decodedMessage);
            var gson = new Gson();
            data = gson.fromJson(decodedMessage, JsonObject.class);
        } catch (Exception e) {
            return logAndReturnBadRequest("Invalid Pub/Sub message: data property is not valid base64 encoded JSON", e);
        }

        var fileName = data.get("name");
        var bucketName = data.get("bucket");
        // Validate if the message is a Cloud Storage event
        if (Objects.isNull(fileName) || Objects.isNull(bucketName)) {
            return logAndReturnBadRequest("Invalid Cloud Storage notification: expected name and bucket properties");
        }

        // Validate if file has avro extension
        if (!fileName.getAsString().endsWith(AVRO_FILE_EXT)) {
            log.info("File {} was skipped from processing due to the wrong extension", fileName.getAsString());
            return new ResponseEntity(HttpStatus.OK);
        }

        var blobInfo = BlobInfo.newBuilder(bucketName.getAsString(), fileName.getAsString()).build();
        return getResponse(gcsFileProcessorService.processFileToBigQuery(blobInfo));
    }

    /**
     * Method waits and checks results of completion of all completable futures
     *
     * @param completableFutures - list of CompletableFutures that contain boolean result of the BigQuery uploading jobs
     * @return HttpStatus.OK if all jobs were successfully finished, HttpStatus.BAD_REQUEST otherwise
     */
    private ResponseEntity getResponse(List<CompletableFuture<Boolean>> completableFutures) {
        CompletableFuture<Void> allFutures = CompletableFuture.allOf(completableFutures.toArray(CompletableFuture[]::new));

        CompletableFuture<Boolean> allCompletableFuture = allFutures
                .thenApply(future -> {
                    return completableFutures.stream()
                            .map(completableFuture -> completableFuture.join())
                            .collect(Collectors.toList());
                }).thenApply(results -> results.stream().allMatch(Boolean.TRUE::equals));

        if (allCompletableFuture.join()) {
            return new ResponseEntity(HttpStatus.OK);
        } else {
            return new ResponseEntity("Error(s) occurred during file processing", HttpStatus.BAD_REQUEST);
        }
    }

    private ResponseEntity logAndReturnBadRequest(String errorMsg) {
        log.error(errorMsg);
        return new ResponseEntity(errorMsg, HttpStatus.BAD_REQUEST);
    }

    private ResponseEntity logAndReturnBadRequest(String errorMsg, Exception e) {
        log.error(errorMsg, e);
        return new ResponseEntity(errorMsg, HttpStatus.BAD_REQUEST);
    }
}