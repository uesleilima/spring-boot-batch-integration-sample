package com.uesleilima.spring.batch.integration.config;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.JobRegistry;
import org.springframework.batch.core.configuration.support.JobRegistryBeanPostProcessor;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.launch.NoSuchJobException;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.batch.integration.launch.JobLaunchingMessageHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.DelayerEndpointSpec;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.core.Pollers;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.FileWritingMessageHandler;
import org.springframework.integration.file.filters.AcceptOnceFileListFilter;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.file.support.FileExistsMode;
import org.springframework.integration.gateway.GatewayProxyFactoryBean;
import org.springframework.integration.scheduling.PollerMetadata;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.messaging.MessageChannel;

import com.uesleilima.spring.batch.integration.domain.EntryRepository;

/**
 * @author Ueslei Lima
 *
 */
@Configuration
@EnableIntegration
public class IntegrationConfig {

	private static final Logger log = LoggerFactory.getLogger(IntegrationConfig.class);

	public static final Long INPUT_DIRECTORY_POOL_RATE = (long) 1000;
	public static final String INPUT_DIRECTORY = "C:\\Temp\\file-process-sample\\input";
	public static final String PROCESSED_DIRECTORY = "C:\\Temp\\file-process-sample\\processed";

	@Autowired
	private JobLauncher jobLauncher;

	@Autowired
	private JobRegistry jobRegistry;

	@Autowired
	private EntryRepository repository;

	@Bean
	public JobRegistryBeanPostProcessor jobRegistryInitializer() {
		JobRegistryBeanPostProcessor initializer = new JobRegistryBeanPostProcessor();
		initializer.setJobRegistry(jobRegistry);
		return initializer;
	}

	@Bean(name = PollerMetadata.DEFAULT_POLLER)
	public PollerMetadata poller() {
		return Pollers.fixedRate(INPUT_DIRECTORY_POOL_RATE).get();
	}
	
	@Bean
	public MessageChannel fileInputChannel() {
		return MessageChannels.direct().get();
	}
	
	@Bean
	public MessageChannel jobRequestChannel() {
		return MessageChannels.direct().get();
	}
	
	@Bean
	public MessageChannel jobExecutionChannel() {
		return MessageChannels.queue().get();
	}
	
	@Bean
	public MessageChannel jobStatusChannel() {
		return MessageChannels.publishSubscribe().get();
	}
	
	@Bean
	public MessageChannel jobRestartChannel() {
		return MessageChannels.queue().get();
	}
	
	@Bean
	public MessageChannel jobExecutionNotifiedChannel() {
		return MessageChannels.queue().get();
	}
	
	@Bean
	public MessageChannel jobCompletedChannel() {
		return MessageChannels.queue().get();
	}

	@Bean
	public IntegrationFlow processFilesFlow() {
		return IntegrationFlows.from(fileReadingMessageSource(), c -> c.poller(poller()))
				.channel(fileInputChannel())
				.<File, JobLaunchRequest>transform(f -> transformFileToRequest(f))
				.channel(jobRequestChannel())
				.handle(jobRequestHandler())
				.<JobExecution, String>transform(e -> transformJobExecutionToStatus(e))
				.channel(jobStatusChannel())
				.get();
	}
	
	@Bean
	public IntegrationFlow processExecutionsFlow() {
		return IntegrationFlows.from(jobExecutionChannel())
				.route(JobExecution.class, e -> e.getStatus().equals(BatchStatus.FAILED),
						m -> m.subFlowMapping(true, f -> f.channel(jobRestartChannel()))
							  .subFlowMapping(false, f -> f.channel(jobExecutionNotifiedChannel())))
				.get();
	}
	
	@Bean
	public IntegrationFlow processExecutionRestartsFlow() {
		return IntegrationFlows.from(jobRestartChannel())
				.delay("wait5sec", (DelayerEndpointSpec e) -> e.defaultDelay(5000))
				.handle(e -> jobRestarter((JobExecution)e.getPayload()))
				.get();
	}
	
	@Bean
	public IntegrationFlow processNotifiedExecutionFlow() {
		return IntegrationFlows.from(jobExecutionNotifiedChannel())
				.route(JobExecution.class, e -> e.getStatus().equals(BatchStatus.COMPLETED),
						m -> m.subFlowMapping(true, f -> f.channel(jobCompletedChannel()))
							  .subFlowMapping(false, f -> f.<JobExecution, String>transform(e -> transformJobExecutionToStatus(e))
														   .channel(jobStatusChannel())))
				
				.get();
	}
	
	@Bean
	public IntegrationFlow processExecutionCompletedFlow() {
		return IntegrationFlows.from(jobCompletedChannel())
				.transform(jobExecutionToFileTransformer())
				.handle(processedFileWritingHandler())
				.channel(jobStatusChannel())
				.get();
	}

	@Bean
	public GatewayProxyFactoryBean jobExecutionsListener(){
	     GatewayProxyFactoryBean factoryBean = new GatewayProxyFactoryBean(JobExecutionListener.class);
	     factoryBean.setDefaultRequestChannel(jobExecutionChannel());
	     return factoryBean;
	}

	@Bean
	public MessageSource<File> fileReadingMessageSource() {
		CompositeFileListFilter<File> filters = new CompositeFileListFilter<>();
		filters.addFilter(new SimplePatternFileListFilter("*.txt"));
		filters.addFilter(new AcceptOnceFileListFilter<>());

		FileReadingMessageSource source = new FileReadingMessageSource();
		source.setAutoCreateDirectory(true);
		source.setDirectory(new File(INPUT_DIRECTORY));
		source.setFilter(filters);

		return source;
	}

	@Bean
	public JobLaunchingMessageHandler jobRequestHandler() {
		return new JobLaunchingMessageHandler(jobLauncher);
	}
	
	@Bean
	public FileWritingMessageHandler processedFileWritingHandler() {
		FileWritingMessageHandler handler = new FileWritingMessageHandler(new File(PROCESSED_DIRECTORY));
		handler.setAutoCreateDirectory(true);
		handler.setDeleteSourceFiles(true);
		handler.setFileExistsMode(FileExistsMode.REPLACE);
		return handler;
	}	
	
	public JobExecution jobRestarter(JobExecution execution) {
		log.info("Restarting job...");
		try {
			Job job = jobRegistry.getJob(execution.getJobInstance().getJobName());
			return jobLauncher.run(job, execution.getJobParameters());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
	
	@Bean
	public GenericTransformer<JobExecution, File> jobExecutionToFileTransformer() {
		return new GenericTransformer<JobExecution, File>(){
			@Override
			public File transform(JobExecution source) {
				String path = source.getJobParameters().getString("input.file.path");
				return new File(path);
			}
		};
	}

	public JobLaunchRequest transformFileToRequest(File file) {
		log.info("Creating request");

		Job job = getJobByFileName(file);
		log.info("Job = " + job.getName());

		JobParametersBuilder paramsBuilder = new JobParametersBuilder();
		paramsBuilder.addDate("start.date", new Date());
		paramsBuilder.addString("input.file.path", file.getPath());

		log.info("Parameters = " + paramsBuilder.toString());

		JobLaunchRequest request = new JobLaunchRequest(job, paramsBuilder.toJobParameters());
		return request;
	}

	public String transformJobExecutionToStatus(JobExecution execution) {
		DateFormat formatter = new SimpleDateFormat("yyyy/MM/dd hh:mm:ss.SS");
		StringBuilder builder = new StringBuilder();

		builder.append(execution.getJobInstance().getJobName());

		BatchStatus evaluatedStatus = endingBatchStatus(execution);
		if (evaluatedStatus == BatchStatus.COMPLETED || evaluatedStatus.compareTo(BatchStatus.STARTED) > 0) {
			builder.append(" has completed with a status of " + execution.getStatus().name() + " at "
					+ formatter.format(new Date()));
			
			builder.append(" with " + repository.count() + " processed records.");
		} else {
			builder.append(" has started at " + formatter.format(new Date()));
		}

		return builder.toString();
	}

	private BatchStatus endingBatchStatus(JobExecution execution) {
		BatchStatus status = execution.getStatus();
		Collection<StepExecution> stepExecutions = execution.getStepExecutions();

		if (stepExecutions.size() > 0) {
			for (StepExecution stepExecution : stepExecutions) {
				if (stepExecution.getStatus().equals(BatchStatus.FAILED)) {
					status = BatchStatus.FAILED;
					break;
				} else {
					status = BatchStatus.COMPLETED;
				}
			}
		}

		return status;
	}

	private Job getJobByFileName(File file) {
		try {
			return jobRegistry.getJob(BatchConfig.JOB_NAME);
		} catch (NoSuchJobException e) {
			log.error(e.getLocalizedMessage());
			e.printStackTrace();
			return null;
		}
	}
}
