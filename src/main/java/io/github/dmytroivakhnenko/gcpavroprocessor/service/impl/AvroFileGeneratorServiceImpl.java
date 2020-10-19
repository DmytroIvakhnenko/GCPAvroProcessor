package io.github.dmytroivakhnenko.gcpavroprocessor.service.impl;

import io.github.dmytroivakhnenko.gcpavroprocessor.service.AvroFileGeneratorService;
import io.github.dmytroivakhnenko.gcpavroprocessor.util.AvroFileGenerator;
import org.springframework.stereotype.Service;

@Service
public class AvroFileGeneratorServiceImpl implements AvroFileGeneratorService {
    @Override
    public void generate(final String name, final int fileCount, final int clientsCount) {
        AvroFileGenerator avroFileGenerator = new AvroFileGenerator();
        avroFileGenerator.generateToTestFolder(name, fileCount, clientsCount);
    }
}
