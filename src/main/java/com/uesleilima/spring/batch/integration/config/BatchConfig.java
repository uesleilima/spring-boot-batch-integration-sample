package com.uesleilima.spring.batch.integration.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.Resource;

import com.uesleilima.spring.batch.integration.domain.Entry;
import com.uesleilima.spring.batch.integration.domain.EntryFieldSetMapper;
import com.uesleilima.spring.batch.integration.domain.EntryRepository;

/**
 * @author Ueslei Lima
 *
 */
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
	@StepScope
	public FlatFileItemReader<Entry> reader(
			@Value("file:///#{jobParameters['input.file.path']}") Resource resource) throws Exception {
		
		DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
		tokenizer.setNames(new String[] { "source", "destination", "amount", "date" });

		DefaultLineMapper<Entry> mapper = new DefaultLineMapper<>();
		mapper.setLineTokenizer(tokenizer);
		mapper.setFieldSetMapper(new EntryFieldSetMapper());
		mapper.afterPropertiesSet();

		FlatFileItemReader<Entry> reader = new FlatFileItemReader<>();
		reader.setResource(resource);
		reader.setLinesToSkip(1);
		reader.setLineMapper(mapper);
		reader.afterPropertiesSet();
		
		return reader;
	}
	
	@Bean
	public ItemProcessor<Entry, Entry> processor(){
		return new ItemProcessor<Entry, Entry>(){

			@Override
			public Entry process(Entry item) throws Exception {
				log.info("Processing: " + item);
				return item;
			}
			
		};
	}

	@Bean
	public RepositoryItemWriter<Entry> writer() {
		RepositoryItemWriter<Entry> writer = new RepositoryItemWriter<>();
		writer.setRepository(repository);
		writer.setMethodName("save");
		return writer;
	}

	@Bean
	public Step processingStepBean(ItemReader<Entry> reader, ItemProcessor<Entry, Entry> processor, ItemWriter<Entry> writer) {
		log.debug("Configuring Step: " + STEP_NAME);
		return stepBuilderFactory.get(STEP_NAME)
				.<Entry, Entry>chunk(5)
				.reader(reader)
				.processor(processor)
				.writer(writer)
				.build();
	}

	@Bean
	public Job processingJobBean(Step processingStep, JobExecutionListener listener) {
		log.debug("Configuring Job: " + JOB_NAME);
		return jobBuilderFactory.get(JOB_NAME)
				.listener(listener)
				.incrementer(new RunIdIncrementer())
				.flow(processingStep)
				.end()
				.build();
	}

}
