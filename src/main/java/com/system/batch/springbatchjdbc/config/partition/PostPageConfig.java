package com.system.batch.springbatchjdbc.config.partition;

import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import com.system.batch.springbatchjdbc.entity.Post;

import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class PostPageConfig {

	public static final Integer CHUNK_SIZE = 5;

	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final EntityManagerFactory entityManagerFactory;
	private final PostRangePartitioner partitioner;
	private final TaskExecutor partitionTaskExecutor;

	@Bean
	public Job postJob(Step managerStep) {
		return new JobBuilder("postJob", jobRepository)
			.start(managerStep)
			.incrementer(new RunIdIncrementer())
			.build();
	}

	@Bean
	public Step managerStep(Step workerStep) {
		return new StepBuilder("managerStep", jobRepository)
			.partitioner("workerStep", partitioner)
			.step(workerStep)
			.taskExecutor(partitionTaskExecutor)
			.gridSize(2) // 파티션 수
			.build();
	}

	@Bean
	public Step workStep(
		JpaPagingItemReader<Post> postReader,
		ItemProcessor<Post, Post> postProcessor,
		JpaItemWriter<Post> postWriter
	) {
		return new StepBuilder("workStep", jobRepository)
			.<Post, Post>chunk(CHUNK_SIZE, transactionManager)
			.reader(postReader)
			.processor(postProcessor)
			.writer(postWriter)
			.allowStartIfComplete(true)
			.build();
	}

	@Bean
	@StepScope
	public JpaPagingItemReader<Post> postReader(
		@Value("#{stepExecutionContext['lastId']}") Integer lastId
	) {
		return new JpaPagingItemReaderBuilder<Post>()
			.name("postReader")
			.entityManagerFactory(entityManagerFactory)
			.queryString("""
				SELECT p FROM Post p
				WHERE p.id <= :lastId
				ORDER BY p.id ASC
				""")
			.parameterValues(Map.of(
				"lastId", lastId
			))
			.pageSize(CHUNK_SIZE)
			.transacted(false)
			.build();
	}

	@Bean
	public ItemProcessor<Post, Post> postProcessor() {
		return item -> {
			boolean isEven = item.getId() % 2 == 0;
			if (!isEven) {
				return null;
			}
			item.addViews();
			return item;
		};
	}

	@Bean
	public JpaItemWriter<Post> postWriter() {
		return new JpaItemWriterBuilder<Post>()
			.entityManagerFactory(entityManagerFactory)
			.build();
	}

}
