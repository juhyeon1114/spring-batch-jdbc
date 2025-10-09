package com.system.batch.springbatchjdbc.config.faulttolerant;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.springframework.batch.core.ItemWriteListener;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
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
public class PostPageFaultTolerantConfig {
	private final JobRepository jobRepository;
	private final PlatformTransactionManager transactionManager;
	private final EntityManagerFactory entityManagerFactory;
	private final AtomicInteger stepCounter = new AtomicInteger(1);
	private final AtomicInteger retryCounter = new AtomicInteger(0);

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
			.faultTolerant()
			.skip(IllegalStateException.class) // 스킵할 예외 객체
			.skipLimit(3) // 한 Step 내에서 스킵 임계치
			.retry(IllegalAccessException.class) // 재시도할 예외 객체
			.retryLimit(2) // 한 Item 기준의 재시도 임계치 (retryLimit에 도달하면, 실패 처리)
			// .processorNonTransactional() // ItemProcessor 비트래잭션 처리: 한 번 처리된 아이템의 결과를 캐시에 저장 → 재시도가 발생할 때 이미 성공한 아이템들은 캐시된 결과를 재사용하고, 실패한 아이템에 대해서만 process()를 다시 호출
			.listener(new ItemWriteListener<>() {
				@Override
				public void beforeWrite(Chunk<? extends Post> items) {
					retryCounter.set(0);
				}

				@Override
				public void afterWrite(Chunk<? extends Post> items) {
					stepCounter.incrementAndGet();
				}
			})
			.allowStartIfComplete(true)
			.build();
	}

	@Bean
	public JpaPagingItemReader<Post> postReader() {
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
			.transacted(false)
			.build();
	}

	@Bean
	public ItemProcessor<Post, Post> postProcessor() {
		return item -> {
			if (item.getId() == 2) {
				throw new IllegalStateException("예외 발생!! (%s)".formatted(retryCounter.incrementAndGet()));
			}
			if (item.getId() == 5) {
				throw new IllegalArgumentException("예외 발생!! (%s)".formatted(retryCounter.incrementAndGet()));
			}
			if (item.getId() == 7 && retryCounter.get() == 0) {
				throw new IllegalAccessException("예외 발생!! (%s)".formatted(retryCounter.incrementAndGet()));
			}
			if (item.getId() == 8) {
				throw new IllegalAccessException("예외 발생!! (%s)".formatted(retryCounter.incrementAndGet()));
			}

			boolean isEven = item.getId() % 2 == 0;
			if (!isEven) {
				return null;
			}
			item.addViews();
			return item;
		};
	}

	@Bean
	public ItemWriter<Post> postWriter() {
		return items -> {
			List<Long> ids = items.getItems().stream().map(Post::getId).toList();
			log.info("쓰기: {}", ids);
		};
	}

	// @Bean
	// public JpaItemWriter<Post> postWriter() {
	// 	return new JpaItemWriterBuilder<Post>()
	// 		.entityManagerFactory(entityManagerFactory)
	// 		.build();
	// }

}
