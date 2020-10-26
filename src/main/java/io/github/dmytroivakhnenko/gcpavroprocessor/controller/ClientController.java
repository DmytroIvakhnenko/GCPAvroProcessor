package io.github.dmytroivakhnenko.gcpavroprocessor.controller;

import io.github.dmytroivakhnenko.gcpavroprocessor.service.GCSFileProcessorService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClientController {
    private final GCSFileProcessorService gcsFileProcessorService;

    public ClientController(GCSFileProcessorService gcsFileProcessorService) {
        this.gcsFileProcessorService = gcsFileProcessorService;
    }

    @GetMapping("/")
    public ResponseEntity<String> welcome() {
        return new ResponseEntity<>("Welcome to GSC AVRO processor application", HttpStatus.OK);
    }

    @GetMapping("/generate")
    public ResponseEntity<String> generate(@RequestParam(required = false, defaultValue = "test") String name, @RequestParam(required = false, defaultValue = "1") int fileCount, @RequestParam(required = false, defaultValue = "1") int clientsCount) {
        gcsFileProcessorService.generateRandomAvroFiles(name, fileCount, clientsCount);
        return new ResponseEntity<>(String.format("%d file(s) with name %s and %d clients was(were) created", fileCount, name, clientsCount), HttpStatus.OK);
    }
}
