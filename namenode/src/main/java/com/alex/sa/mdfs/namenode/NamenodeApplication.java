package com.alex.sa.mdfs.namenode;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@EnableEurekaClient
@RestController
public class NamenodeApplication {

	public static void main(String[] args) {
		SpringApplication.run(NamenodeApplication.class, args);
	}

	@GetMapping("/")
	public String listFileSystem() {
		return "test";
	}
}
