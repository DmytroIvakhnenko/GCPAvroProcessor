package io.github.dmytroivakhnenko.gcpavroprocessor.util;

import example.gcp.Client;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class AvroFileGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(AvroFileGenerator.class);
    private static final String AVRO_FILE_EXT = ".avro";
    private static final String AVRO_FILE_PROJECT_PATH = "/src/test/resources/avro/";
    private static final int NAME_LENGTH = 10;
    private static final int PHONE_LENGTH = 8;
    private static final int ADDRESS_LENGTH = 30;

    public static Client createRandomClient() {
        var client = new Client();
        client.setId(RandomUtils.nextLong());
        client.setName(RandomStringUtils.randomAlphabetic(NAME_LENGTH));
        client.setPhone(RandomStringUtils.randomNumeric(PHONE_LENGTH));
        client.setAddress(RandomStringUtils.randomAlphanumeric(ADDRESS_LENGTH));
        return client;
    }

    public static byte[] createByteArrayOfRandomClient() {
        var client = createRandomClient();
        byte[] avroFileContent = new byte[0];
        DatumWriter<Client> writer = new SpecificDatumWriter<>(Client.class);
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            Encoder byteEncoder = EncoderFactory.get().binaryEncoder(stream, null);
            writer.write(client, byteEncoder);
            byteEncoder.flush();
            avroFileContent = stream.toByteArray();
        } catch (IOException e) {
            LOG.error("Exception occurs during sample avro file generation", e);
        }
        return avroFileContent;
    }

    public void generateToTestFolder(final String name, final int fileCount, final int clientsCount) {
        var client = new Client();
        DatumWriter<Client> clientDatumWriter = new SpecificDatumWriter<>(Client.class);
        try (DataFileWriter<Client> clientDataFileWriter = new DataFileWriter<>(clientDatumWriter)) {
            for (int i = 0; i < fileCount; i++) {
                clientDataFileWriter.create(client.getSchema(), new File(Paths.get("").toAbsolutePath().normalize().toString() + AVRO_FILE_PROJECT_PATH + name + i + AVRO_FILE_EXT));
                for (int j = 0; j < clientsCount; j++) {
                    client = createRandomClient();
                    clientDataFileWriter.append(client);
                }
                clientDataFileWriter.close();
            }
        } catch (IOException e) {
            LOG.error("Exception occurs during avro file generation", e);
        }
    }

    public void generateToGoogleStorage() {
        //TODO
    }

    public static void main(String[] args) {
        AvroFileGenerator ag = new AvroFileGenerator();
        ag.generateToTestFolder("test_5clientss", 1, 5);
    }
}
