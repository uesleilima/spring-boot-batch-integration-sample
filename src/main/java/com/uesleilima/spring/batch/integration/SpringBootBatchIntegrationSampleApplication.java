package com.uesleilima.spring.batch.integration;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.uesleilima.spring.batch.integration.config.IntegrationConfig;

@SpringBootApplication
public class SpringBootBatchIntegrationSampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringBootBatchIntegrationSampleApplication.class, args);
		
		copyFileToTempDir();
	}

	private static void copyFileToTempDir() {
		try {
			Resource resource = new ClassPathResource("data/entries.txt");
			FileUtils.copyFileToDirectory(resource.getFile(), new File(IntegrationConfig.DIRECTORY));
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
