package io.github.dmytroivakhnenko.gcpavroprocessor.service.impl;

import io.github.dmytroivakhnenko.gcpavroprocessor.service.AvroFileGeneratorService;
import io.github.dmytroivakhnenko.gcpavroprocessor.util.AvroFileGenerator;
import org.springframework.stereotype.Service;

@Service
public class AvroFileGeneratorServiceImpl implements AvroFileGeneratorService {
    @Override
    public void generate(String prefix, int count) {
        AvroFileGenerator avroFileGenerator = new AvroFileGenerator();
        avroFileGenerator.generate("", 1);
    }
}
