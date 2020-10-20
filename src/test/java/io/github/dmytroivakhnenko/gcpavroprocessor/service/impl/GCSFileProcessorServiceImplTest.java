package io.github.dmytroivakhnenko.gcpavroprocessor.service.impl;

import com.google.cloud.storage.BlobInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.junit.jupiter.SpringExtension;

@ExtendWith(SpringExtension.class)
public class GCSFileProcessorServiceImplTest {
    private static final Logger LOG = LoggerFactory.getLogger(GCSFileProcessorServiceImplTest.class);
    private static final BlobInfo testBlobInfo = BlobInfo.newBuilder("test_bucket", "test_name").build();

    @InjectMocks
    private GCSFileProcessorServiceImpl gcsFileProcessorService;
}
