package com.alex.sa.mdfs.eurekeserver;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.server.EnableEurekaServer;

@SpringBootApplication
@EnableEurekaServer
public class EurekeserverApplication {

	public static void main(String[] args) {
		SpringApplication.run(EurekeserverApplication.class, args);
	}
}
