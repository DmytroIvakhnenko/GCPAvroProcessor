package io.github.dmytroivakhnenko.gcpavroprocessor.service;

import io.github.dmytroivakhnenko.gcpavroprocessor.util.AvroFileGenerator;
import org.springframework.stereotype.Service;

@Service
public class AvroFileGeneratorServiceImpl implements AvroFileGeneratorService {
    @Override
    public void generate(String prefix, int count) {
        AvroFileGenerator avroFileGenerator = new AvroFileGenerator();
        avroFileGenerator.generate("");
    }
}
