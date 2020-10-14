package io.github.dmytroivakhnenko.gcpavroprocessor.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.util.StreamUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.nio.charset.Charset;

@RestController
public class GCSController {
    @Value("gs://gcp_avro_processor_input_bucket/client_test.avro")
    private Resource gcsFile;

    @RequestMapping(value = "/bucket", method = RequestMethod.GET)
    public String readGcsFile() throws IOException {
        return StreamUtils.copyToString(
                gcsFile.getInputStream(),
                Charset.defaultCharset()) + "\n";
    }

}
