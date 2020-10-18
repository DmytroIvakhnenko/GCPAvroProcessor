package io.github.dmytroivakhnenko.gcpavroprocessor.controller;

import io.github.dmytroivakhnenko.gcpavroprocessor.service.AvroFileGeneratorService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ClientController {
    private final AvroFileGeneratorService avroFileGeneratorService;

    public ClientController(AvroFileGeneratorService avroFileGeneratorService) {
        this.avroFileGeneratorService = avroFileGeneratorService;
    }

    @RequestMapping("/")
    public String test() {
        return "Welcome to GSC AVRO processor start page";
    }

    @GetMapping("/generate")
    public ResponseEntity<Void> generate(@RequestParam(required = false, defaultValue = "test") String name, @RequestParam(required = false, defaultValue = "1") int fileCount, @RequestParam(required = false, defaultValue = "1") int clientsCount) {
        avroFileGeneratorService.generate(name, fileCount, clientsCount);
        return new ResponseEntity<>(HttpStatus.OK);
    }

}
