package io.github.dmytroivakhnenko.gcpavroprocessor.service;

public interface AvroFileGeneratorService {
    void generate(String name, int fileCount, int clientsCount);
}
