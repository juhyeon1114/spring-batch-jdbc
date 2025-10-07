package com.system.batch.springbatchjdbc.config;

import java.util.Map;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import com.system.batch.springbatchjdbc.entity.Post;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * ./gradlew bootRun --args='--spring.batch.job.name=postPageJob'
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class PostPageConfig {
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final DataSource dataSource;

	@Bean
	public Job postPageJob() {
		return new JobBuilder("postPageJob", jobRepository)
			.start(postPageStep())
			.build();
	}

	@Bean
	public Step postPageStep() {
		return new StepBuilder("postPageStep", jobRepository)
			.<Post, Post>chunk(3, transactionManager)
			.reader(postPageReader())
			.processor(postPageProcessor())
			.writer(postWriter())
			.allowStartIfComplete(true)
			.build();
	}

	@Bean
	public JdbcPagingItemReader<Post> postPageReader() {
		return new JdbcPagingItemReaderBuilder<Post>()
			.name("postPageReader")
			.dataSource(dataSource)
			.pageSize(3)
			.selectClause("select *")
			.fromClause("from posts")
			.whereClause("id <= :id")
			.sortKeys(Map.of("id", Order.ASCENDING))
			.parameterValues(Map.of(
				"id", 9
			))
			.beanRowMapper(Post.class)
			.build();
	}

	/**
	 * id가 짝수인 것만 필터링
	 */
	@Bean
	public ItemProcessor<Post, Post> postPageProcessor() {
		return item -> {
			boolean isEven = item.getId() % 2 == 0;
			return isEven ? item : null;
		};
	}

	/**
	 * 조회수를 1증가 시킴
	 */
	@Bean
	public JdbcBatchItemWriter<Post> postWriter() {
		return new JdbcBatchItemWriterBuilder<Post>()
			.dataSource(dataSource)
			.sql("""
				UPDATE posts
				SET views = views + 1,
					updated_at = NOW()
				WHERE id = :id
				""")
			.beanMapped()
			.assertUpdates(true)
			.build();
	}

}
