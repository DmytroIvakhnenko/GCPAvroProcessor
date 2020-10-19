package io.github.dmytroivakhnenko.gcpavroprocessor.service.impl;

import com.google.cloud.storage.BlobInfo;
import io.github.dmytroivakhnenko.gcpavroprocessor.config.BigQueryIntegrationConfig;
import io.github.dmytroivakhnenko.gcpavroprocessor.exception.AvroFileValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(SpringExtension.class)
public class GCSFileProcessorServiceImplTest {
    private static final Logger LOG = LoggerFactory.getLogger(GCSFileProcessorServiceImplTest.class);
    private static final BlobInfo testBlobInfo = BlobInfo.newBuilder("test_bucket", "test_name").build();

    @Mock
    private BigQueryIntegrationConfig.BigQueryFileGateway bigQueryFileGateway;

    @InjectMocks
    private GCSFileProcessorServiceImpl gcsFileProcessorService;

    @Test
    public void whenFileIsNotFollowingSchemaThrowsException() {
        assertThrows(AvroFileValidationException.class, () -> {
            gcsFileProcessorService.getClientsFromAvroFile("invalid avro file content".getBytes(), testBlobInfo);
        });
    }

    @Test
    public void whenFileIsFollowingSchema() throws IOException {
        File file = ResourceUtils.getFile("classpath:avro/test_5clients.avro");
        assertEquals(gcsFileProcessorService.getClientsFromAvroFile(Files.readAllBytes(file.toPath()), testBlobInfo).size(), 5);
        //assertEquals(gcsFileProcessorService.getClientsFromAvroFile(AvroFileGenerator.createByteArrayOfRandomClient(), testBlobInfo).size(), 5);
    }
}
