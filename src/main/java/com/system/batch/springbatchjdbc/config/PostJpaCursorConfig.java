package com.system.batch.springbatchjdbc.config;

import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaCursorItemReader;
import org.springframework.batch.item.database.builder.JpaCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import com.system.batch.springbatchjdbc.entity.Post;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PersistenceContext;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class PostJpaCursorConfig {
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final EntityManagerFactory entityManagerFactory;
	@PersistenceContext
	private EntityManager entityManager;

	@Bean
	public Job postCursorJob() {
		return new JobBuilder("postCursorJob", jobRepository)
			.start(postCursorStep())
			.build();
	}

	@Bean
	public Step postCursorStep() {
		return new StepBuilder("postCursorStep", jobRepository)
			.<Post, Post>chunk(3, transactionManager)
			.reader(postCursorReader())
			.processor(postCursorProcessor())
			.writer(postWriter())
			.allowStartIfComplete(true)
			.build();
	}

	@Bean
	public JpaCursorItemReader<Post> postCursorReader() {
		return new JpaCursorItemReaderBuilder<Post>()
			.name("postCursorReader")
			.entityManagerFactory(entityManagerFactory)
			.queryString("""
				SELECT p FROM Post p
				WHERE p.id <= :id
				ORDER BY p.id ASC
				""")
			.parameterValues(Map.of(
				"id", 9
			))
			.build();
	}

	@Bean
	public ItemProcessor<Post, Post> postCursorProcessor() {
		return item -> {
			boolean isEven = item.getId() % 2 == 0;
			return isEven ? item : null;
		};
	}

	@Bean
	@StepScope
	public ItemWriter<Post> postWriter() {
		return items -> items.forEach(item -> {
			log.info("postWriter: {}", item);
			Post post = entityManager.merge(item);
			post.addViews();
		});
	}

}
