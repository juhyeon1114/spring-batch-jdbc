package com.system.batch.springbatchjdbc.config.partition;

import static com.system.batch.springbatchjdbc.config.partition.PostPageConfig.*;

import java.util.HashMap;
import java.util.Map;

import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@JobScope
@Component
public class PostRangePartitioner implements Partitioner {

	@Override
	public Map<String, ExecutionContext> partition(int gridSize) {
		Map<String, ExecutionContext> partitions = new HashMap<>(gridSize);

		for (int i = 0; i < gridSize; i++) {
			ExecutionContext executionContext = new ExecutionContext();
			int fromId = CHUNK_SIZE * i;
			int toId = CHUNK_SIZE * (i + 1);
			executionContext.putInt("fromId", fromId);
			executionContext.putInt("toId", toId);
			partitions.put("fromId" + fromId, executionContext);
			partitions.put("toId" + toId, executionContext);
		}

		return partitions;
	}

}
