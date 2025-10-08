package com.system.batch.springbatchjdbc.config.querydsl;

import static com.system.batch.springbatchjdbc.entity.QPost.*;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
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
public class PostQueryDslJobConfig {
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final EntityManagerFactory entityManagerFactory;
	@PersistenceContext
	private EntityManager entityManager;

	@Bean
	public Job postJob() {
		return new JobBuilder("postJob", jobRepository)
			.start(postStep())
			.build();
	}

	@Bean
	public Step postStep() {
		return new StepBuilder("postStep", jobRepository)
			.<Post, Post>chunk(3, transactionManager)
			.reader(postReader())
			.processor(postProcessor())
			.writer(postWriter())
			.allowStartIfComplete(true)
			.build();
	}

	@Bean
	public QueryDslPagingItemReader<Post> postReader() {
		return QueryDslPagingItemReader.keyBased(
			"postReader",
			3,
			entityManagerFactory,
			queryFactory -> queryFactory.selectFrom(post).where(post.id.loe(9)).orderBy(post.id.asc()),
			post.id::gt,
			Post::getId
		);
	}

	@Bean
	public ItemProcessor<Post, Post> postProcessor() {
		return item -> {
			boolean isEven = item.getId() % 2 == 0;
			return isEven ? item : null;
		};
	}

	@Bean
	public ItemWriter<Post> postWriter() {
		return items -> items.forEach(item -> {
			log.info("postWriter: {}", item);
			Post post = entityManager.merge(item);
			post.addViews();
		});
	}

}
