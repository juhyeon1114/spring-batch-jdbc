package com.system.batch.springbatchjdbc.config.jpa;

import java.util.Map;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaItemWriterBuilder;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import com.system.batch.springbatchjdbc.entity.Post;

import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@RequiredArgsConstructor
public class PostJpaPageConfig {
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final EntityManagerFactory entityManagerFactory;

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
			.reader(postPageReader())
			.processor(postPageProcessor())
			.writer(postWriter())
			.allowStartIfComplete(true)
			.build();
	}

	@Bean
	public JpaPagingItemReader<Post> postPageReader() {
		return new JpaPagingItemReaderBuilder<Post>()
			.name("postPageReader")
			.entityManagerFactory(entityManagerFactory)
			.queryString("""
				SELECT p FROM Post p
				WHERE p.id <= :id
				ORDER BY p.id ASC
				""")
			.parameterValues(Map.of(
				"id", 9
			))
			.pageSize(3)
			// transacted == true: 데이터 조회 전, entityManager.flush()가 호출됨
			// transacted == false: 데이터 조회 후, entityManager.detach()를 실행하여, 영속성 컨텍스트에서 관리되지 않도록 한다.
			.transacted(false)
			.build();
	}

	@Bean
	public ItemProcessor<Post, Post> postPageProcessor() {
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
