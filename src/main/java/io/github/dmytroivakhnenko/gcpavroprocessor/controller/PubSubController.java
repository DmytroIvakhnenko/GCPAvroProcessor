package io.github.dmytroivakhnenko.gcpavroprocessor.controller;

import com.google.cloud.storage.BlobInfo;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.github.dmytroivakhnenko.gcpavroprocessor.service.GCSFileProcessorService;
import io.github.dmytroivakhnenko.gcpavroprocessor.util.PubSubEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Base64;
import java.util.Optional;

// PubsubController consumes a Pub/Sub message.
@RestController
public class PubSubController {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubController.class);

    private GCSFileProcessorService gcsFileProcessorService;

    public PubSubController(
            GCSFileProcessorService gcsFileProcessorService) {
        this.gcsFileProcessorService = gcsFileProcessorService;
    }

    @RequestMapping(value = "/pubsub", method = RequestMethod.POST)
    public ResponseEntity receiveMessage(@RequestBody PubSubEvent event) {
        // Get PubSub message from request body.
        var payload = Optional.ofNullable(event.getPayload());
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
            data = new JsonParser().parse(decodedMessage).getAsJsonObject();
        } catch (Exception e) {
            var msg = "Error: Invalid Pub/Sub message: data property is not valid base64 encoded JSON";
            LOG.error(msg);
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

        LOG.info("Name = " + data.get("name") + " Bucket = " + data.get("bucket"));

        var blobInfo = BlobInfo.newBuilder(bucketName.get().getAsString(), fileName.get().getAsString()).build();
        gcsFileProcessorService.processFile(blobInfo);

        return new ResponseEntity(HttpStatus.OK);
    }
}