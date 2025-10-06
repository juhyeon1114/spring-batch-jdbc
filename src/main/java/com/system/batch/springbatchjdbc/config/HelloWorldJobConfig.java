package com.system.batch.springbatchjdbc.config;

import java.util.List;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
public class HelloWorldJobConfig {

	private final JdbcTemplate jdbcTemplate;

	public HelloWorldJobConfig(JdbcTemplate jdbcTemplate) {
		this.jdbcTemplate = jdbcTemplate;
	}

	@Bean
	public Job helloWorldJob(JobRepository jobRepository, Step helloWorldStep) {
		return new JobBuilder("helloWorldJob", jobRepository)
			.start(helloWorldStep)
			.build();
	}

	@Bean
	public Step helloWorldStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
		return new StepBuilder("helloWorldStep", jobRepository)
			.tasklet(helloWorldTasklet(), transactionManager)
			.allowStartIfComplete(true)
			.build();
	}

	public Tasklet helloWorldTasklet() {
		return (contribution, chunkContext) -> {
			List<Long> ids = jdbcTemplate.queryForList("SELECT job_execution_id FROM batch_job_execution", Long.class);
			log.info("Hello World! {}", ids.size());
			return RepeatStatus.FINISHED;
		};
	}

}
