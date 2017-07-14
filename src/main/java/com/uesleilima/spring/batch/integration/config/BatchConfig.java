package com.uesleilima.spring.batch.integration.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.uesleilima.spring.batch.integration.domain.Entry;
import com.uesleilima.spring.batch.integration.domain.EntryRepository;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

	private static final Logger log = LoggerFactory.getLogger(BatchConfig.class);

	public static final String STEP_NAME = "processingStep";
	public static final String JOB_NAME = "processingJob";

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private EntryRepository repository;

	@Bean
	private ItemReader<Entry> reader() {
		// TODO Auto-generated method stub
		return null;
	}

	@Bean
	private ItemWriter<Entry> writer() {
		RepositoryItemWriter<Entry> writer = new RepositoryItemWriter<>();
		writer.setRepository(repository);
		writer.setMethodName("save");
		return writer;
	}

	@Bean
	public Step processingStepBean() {
		log.debug("Configuring Step: " + STEP_NAME);
		return stepBuilderFactory.get(STEP_NAME)
				.<Entry, Entry>chunk(1)
				.reader(reader())
				.writer(writer())
				.build();
	}

	@Bean
	public Job processingJobBean(JobExecutionListener listener) {
		log.debug("Configuring Job: " + JOB_NAME);
		return jobBuilderFactory.get(JOB_NAME)
				.incrementer(new RunIdIncrementer())
				.listener(listener)
				.flow(processingStepBean())
				.end()
				.build();
	}

}
