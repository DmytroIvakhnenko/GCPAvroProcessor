package io.github.dmytroivakhnenko.gcpavroprocessor.util;

import example.gcp.Client;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

@Slf4j
public class AvroFileUtils {
    private static final String AVRO_FILE_SAVE_PATH = "/src/test/resources/avro/";
    private static final int NAME_LENGTH = 10;
    private static final int PHONE_LENGTH = 8;
    private static final int ADDRESS_LENGTH = 30;
    public static final String AVRO_FILE_EXT = ".avro";

    public static Client createRandomClient() {
        var client = new Client();
        client.setId(RandomUtils.nextLong());
        client.setName(RandomStringUtils.randomAlphabetic(NAME_LENGTH));
        client.setPhone(RandomStringUtils.randomNumeric(PHONE_LENGTH));
        client.setAddress(RandomStringUtils.randomAlphanumeric(ADDRESS_LENGTH));
        return client;
    }

    public static void generateRandomAvroFilesToLocalFolder(final String name, final int fileCount, final int clientsCount) {
        var client = new Client();
        DatumWriter<Client> clientDatumWriter = new SpecificDatumWriter<>(Client.class);
        try (DataFileWriter<Client> clientDataFileWriter = new DataFileWriter<>(clientDatumWriter)) {
            for (int i = 0; i < fileCount; i++) {
                clientDataFileWriter.create(client.getSchema(), new File(Paths.get("").toAbsolutePath().normalize().toString() + AVRO_FILE_SAVE_PATH + name + i + AVRO_FILE_EXT));
                for (int j = 0; j < clientsCount; j++) {
                    client = createRandomClient();
                    clientDataFileWriter.append(client);
                }
                clientDataFileWriter.close();
            }
        } catch (IOException e) {
            log.error("Exception occurs during avro file generation", e);
        }
    }

    public static void main(String[] args) {
        generateRandomAvroFilesToLocalFolder("test_300_clients", 1, 6_000_000);
    }
}
