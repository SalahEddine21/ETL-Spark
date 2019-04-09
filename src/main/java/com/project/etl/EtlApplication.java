package com.project.etl;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class EtlApplication {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "C:\\winutils\\");
		SpringApplication.run(EtlApplication.class, args);
	}
}
