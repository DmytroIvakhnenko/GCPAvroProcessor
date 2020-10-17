package io.github.dmytroivakhnenko.gcpavroprocessor.service.impl;


import com.google.cloud.storage.*;
import example.gcp.Client;
import io.github.dmytroivakhnenko.gcpavroprocessor.service.GCSFileProcessorService;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;

@Service
public class GCSFileProcessorServiceImpl implements GCSFileProcessorService {
    private static final Logger LOG = LoggerFactory.getLogger(GCSFileProcessorServiceImpl.class);
    private static Storage storage = StorageOptions.getDefaultInstance().getService();

    @Override
    public void processFile(BlobInfo blobInfo) {
        var blob = storage.get(BlobId.of(blobInfo.getBucket(), blobInfo.getName()));

        LOG.info(
                "File " + new String(blob.getContent()) + " received by the non-streaming inbound "
                        + "channel adapter.");
        var client = deserialize(blob);
        LOG.info("Deserialized file: " + client.toString());
    }

    private Client deserialize(Blob blob) {
        //DeSerializing the objects
        DatumReader<Client> empDatumReader = new SpecificDatumReader<>(Client.class);

        Decoder decoder = DecoderFactory.get().binaryDecoder(blob.getContent(), null);
        Client client = null;
        try {
            client = empDatumReader.read(null, decoder);
        } catch (IOException e) {
            LOG.error("Exception occurs during avro file deserialization", e);
        }
        return client;
    }
}
