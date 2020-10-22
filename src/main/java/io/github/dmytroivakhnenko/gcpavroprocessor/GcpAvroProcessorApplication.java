package io.github.dmytroivakhnenko.gcpavroprocessor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.integration.IntegrationAutoConfiguration;

@SpringBootApplication(exclude = IntegrationAutoConfiguration.class)
public class GcpAvroProcessorApplication {

    public static void main(String[] args) {
        SpringApplication.run(GcpAvroProcessorApplication.class, args);
    }

}
