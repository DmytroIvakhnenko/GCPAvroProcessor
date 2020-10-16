package io.github.dmytroivakhnenko.gcpavroprocessor.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

@Configuration
@EnableWebMvc
@ComponentScan(basePackages = "io.github.dmytroivakhnenko.gcpavroprocessor.controller")

public class WebConfig {

}