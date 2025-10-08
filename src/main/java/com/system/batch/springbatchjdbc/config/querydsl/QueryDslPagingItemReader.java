package com.system.batch.springbatchjdbc.config.querydsl;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import org.springframework.batch.item.database.AbstractPagingItemReader;
import org.springframework.util.CollectionUtils;

import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.jpa.impl.JPAQuery;
import com.querydsl.jpa.impl.JPAQueryFactory;

import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;

public class QueryDslPagingItemReader<T> extends AbstractPagingItemReader<T> {

	private final EntityManager entityManager;
	private final JPAQueryFactory queryFactory;
	private final Function<JPAQueryFactory, JPAQuery<T>> querySupplier;
	private final Function<Long, BooleanExpression> keyExpressionSupplier;
	private final Function<T, Long> keyExtractor;
	private final boolean keyBased;
	private Long lastId;

	protected QueryDslPagingItemReader(
		String name,
		int chunkSize,
		EntityManagerFactory entityManagerFactory,
		Function<JPAQueryFactory, JPAQuery<T>> querySupplier,
		Function<Long, BooleanExpression> keyExpressionSupplier,
		Function<T, Long> keyExtractor,
		boolean keyBased
	) {
		super.setPageSize(chunkSize);
		setName(name);
		this.querySupplier = querySupplier;
		this.keyExpressionSupplier = keyExpressionSupplier;
		this.keyExtractor = keyExtractor;
		this.entityManager = entityManagerFactory.createEntityManager();
		this.queryFactory = new JPAQueryFactory(entityManager);
		this.keyBased = keyBased;
	}

	public static <R> QueryDslPagingItemReader<R> keyBased(
		String name,
		int chunkSize,
		EntityManagerFactory entityManagerFactory,
		Function<JPAQueryFactory, JPAQuery<R>> query,
		Function<Long, BooleanExpression> keyExpressionSupplier,
		Function<R, Long> keyExtractor
	) {
		return new QueryDslPagingItemReader<>(
			name,
			chunkSize,
			entityManagerFactory,
			query,
			keyExpressionSupplier,
			keyExtractor,
			true
		);
	}

	public static <R> QueryDslPagingItemReader<R> offsetBased(
		String name,
		int chunkSize,
		EntityManagerFactory entityManagerFactory,
		Function<JPAQueryFactory, JPAQuery<R>> querySupplier
	) {
		return new QueryDslPagingItemReader<>(
			name,
			chunkSize,
			entityManagerFactory,
			querySupplier,
			null,
			null,
			false
		);
	}

	@Override
	protected void doClose() throws Exception {
		if (entityManager != null) {
			entityManager.close();
		}
		super.doClose();
	}

	@Override
	protected void doReadPage() {
		// 초기화
		if (CollectionUtils.isEmpty(results)) {
			results = new CopyOnWriteArrayList<>();
		} else {
			results.clear();
		}

		// 쿼리
		long offset = keyBased ? 0 : ((long)getPage() * getPageSize());
		BooleanExpression keyExpression = keyBased && lastId != null ? keyExpressionSupplier.apply(lastId) : null;
		List<T> queryResults = querySupplier.apply(queryFactory)
			.where(keyExpression)
			.offset(offset)
			.limit(getPageSize())
			.fetch();

		// 쿼리 결과 처리
		for (T queryResult : queryResults) {
			entityManager.detach(queryResult);
			results.add(queryResult);
		}

		// 결과 후 처리
		if (!results.isEmpty() && keyBased) {
			lastId = keyExtractor.apply(results.getLast());
		}
	}

}
