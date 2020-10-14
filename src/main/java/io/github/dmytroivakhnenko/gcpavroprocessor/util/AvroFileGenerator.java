package io.github.dmytroivakhnenko.gcpavroprocessor.util;

import example.gcp.Client;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class AvroFileGenerator {
    private static final Logger LOG = LoggerFactory.getLogger(AvroFileGenerator.class);
    private static final String AVRO_FILE_NAME = "client_test.avro";
    private static final int NAME_LENGTH = 10;
    private static final int PHONE_LENGTH = 8;
    private static final int ADDRESS_LENGTH = 30;

    private Client createRandomClient() {
        Client client = new Client();
        client.setId(RandomUtils.nextLong());
        client.setName(RandomStringUtils.randomAlphabetic(NAME_LENGTH));
        client.setPhone(RandomStringUtils.randomNumeric(PHONE_LENGTH));
        client.setAddress(RandomStringUtils.randomAlphanumeric(ADDRESS_LENGTH));
        return client;
    }

    public void generate(final String prefix, final int count) {
        Client client = createRandomClient();
        DatumWriter<Client> clientDatumWriter = new SpecificDatumWriter<>(Client.class);
        try (DataFileWriter<Client> clientDataFileWriter = new DataFileWriter<>(clientDatumWriter)) {
            clientDataFileWriter.create(client.getSchema(), new File(Paths.get("").toAbsolutePath().normalize().toString() + "/src/test/resources/avro/" + prefix + AVRO_FILE_NAME));
            clientDataFileWriter.append(client);
        } catch (IOException e) {
            LOG.error("Exception occurs during avro file generation", e);
        }
    }

    public static void main(String[] args) {
        AvroFileGenerator ag = new AvroFileGenerator();
        ag.generate("", 1);
    }
}
