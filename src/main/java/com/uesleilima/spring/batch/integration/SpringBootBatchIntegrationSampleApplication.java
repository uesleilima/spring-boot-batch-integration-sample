package com.uesleilima.spring.batch.integration;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

import com.uesleilima.spring.batch.integration.config.IntegrationConfig;

/**
 * @author Ueslei Lima
 *
 */
@SpringBootApplication
public class SpringBootBatchIntegrationSampleApplication {	
	
	private static final Logger log = LoggerFactory.getLogger(SpringBootBatchIntegrationSampleApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(SpringBootBatchIntegrationSampleApplication.class, args);
		
		copySampleFileToInputDir();
	}

	private static void copySampleFileToInputDir() {
		try {
			log.info("Copying sample file to be processed in dir: " + IntegrationConfig.INPUT_DIRECTORY);
			Resource resource = new ClassPathResource("data/entries.txt");
			FileUtils.copyFileToDirectory(resource.getFile(), new File(IntegrationConfig.INPUT_DIRECTORY));
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
