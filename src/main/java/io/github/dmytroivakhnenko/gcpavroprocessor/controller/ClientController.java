package io.github.dmytroivakhnenko.gcpavroprocessor.controller;

import io.github.dmytroivakhnenko.gcpavroprocessor.service.AvroFileGeneratorService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.net.InetAddress;
import java.net.UnknownHostException;

@RestController
public class ClientController {
    private final AvroFileGeneratorService avroFileGeneratorService;

    public ClientController(AvroFileGeneratorService avroFileGeneratorService) {
        this.avroFileGeneratorService = avroFileGeneratorService;
    }

    @RequestMapping("/test")
    public String test() {
        StringBuilder message = new StringBuilder("Google Cloud Build test!");
        try {
            InetAddress ip = InetAddress.getLocalHost();
            message.append(" From host: ").append(ip);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return message.toString();
    }

    @GetMapping("/generate")
    public ResponseEntity<Void> generate(@RequestParam(required = false, defaultValue = "test") String prefix, @RequestParam(required = false, defaultValue = "1") int count) {
        avroFileGeneratorService.generate(prefix, count);
        return new ResponseEntity<>(HttpStatus.OK);
    }

}
