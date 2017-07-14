package com.uesleilima.spring.batch.integration.config;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.integration.launch.JobLaunchRequest;
import org.springframework.batch.integration.launch.JobLaunchingMessageHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.config.EnableIntegration;

@Configuration
@EnableIntegration
public class IntegrationConfig {
	
	@Autowired
	private JobLauncher jobLauncher;

	@ServiceActivator(inputChannel = "job-requests", outputChannel = "job-statuses")
	public JobLaunchingMessageHandler handler() {
		return new JobLaunchingMessageHandler(jobLauncher);
	}

	@ServiceActivator
	public JobExecution launch(JobLaunchRequest request) throws JobExecutionAlreadyRunningException,
			JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException {

		Job job = request.getJob();
		JobParameters jobParameters = request.getJobParameters();

		return jobLauncher.run(job, jobParameters);
	}
}
