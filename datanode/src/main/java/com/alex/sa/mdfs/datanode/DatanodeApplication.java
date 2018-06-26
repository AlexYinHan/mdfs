package com.alex.sa.mdfs.datanode;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import com.alex.sa.mdfs.datanode.storage.*;

@SpringBootApplication
@EnableConfigurationProperties(StorageProperties.class)
public class DatanodeApplication {

	public static void main(String[] args) {
		SpringApplication.run(DatanodeApplication.class, args);
	}

	@Bean
	CommandLineRunner init(StorageService storageService) {
		return (args) -> {
			storageService.deleteAll();
			storageService.init();
		};
	}
}
