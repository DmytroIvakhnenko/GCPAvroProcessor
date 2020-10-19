package io.github.dmytroivakhnenko.gcpavroprocessor.service.impl;


import example.gcp.Client;
import io.github.dmytroivakhnenko.gcpavroprocessor.config.BigQueryIntegrationConfig;
import io.github.dmytroivakhnenko.gcpavroprocessor.util.AvroFileGenerator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
public class GCSFileProcessorServiceImplTest {
    private static final Logger LOG = LoggerFactory.getLogger(GCSFileProcessorServiceImplTest.class);


    @Mock
    private BigQueryIntegrationConfig.BigQueryFileGateway bigQueryFileGateway;

    @InjectMocks
    private GCSFileProcessorServiceImpl gcsFileProcessorService;

    @Test
    public void whenFileIsNotFollowingSchemaReturnFalse() {
        assertFalse(gcsFileProcessorService.isValidatAvroFileAccordingToSchema("invalid avro file content".getBytes(), Client.SCHEMA$));
    }

    @Test
    public void whenFileIsFollowingSchemaReturnTrue() {
        assertTrue(gcsFileProcessorService.isValidatAvroFileAccordingToSchema(AvroFileGenerator.createByteArrayOfRandomClient(), Client.SCHEMA$));
    }
}
