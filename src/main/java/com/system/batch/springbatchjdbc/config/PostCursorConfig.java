package com.system.batch.springbatchjdbc.config;

import java.util.List;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;

import com.system.batch.springbatchjdbc.entity.Post;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * ./gradlew bootRun --args='--spring.batch.job.name=postCursorJob'
 */
@Slf4j
@Configuration
@RequiredArgsConstructor
public class PostCursorConfig {
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final DataSource dataSource;

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
	public JdbcCursorItemReader<Post> postCursorReader() {
		return new JdbcCursorItemReaderBuilder<Post>()
			.name("postCursorReader")
			.dataSource(dataSource)
			.sql("SELECT * FROM posts WHERE id <= ? ORDER BY id ASC")
			.queryArguments(List.of(9))
			.beanRowMapper(Post.class)
			// .rowMapper((rs, rowNum) -> {
			// 	// 맵핑 직접 구현 가능
			// })
			.build();
	}

	/**
	 * id가 짝수인 것만 필터링
	 */
	@Bean
	public ItemProcessor<Post, Post> postCursorProcessor() {
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
