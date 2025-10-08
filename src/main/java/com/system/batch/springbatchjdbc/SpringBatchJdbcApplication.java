package com.system.batch.springbatchjdbc;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan(basePackages = {"com.system.batch.springbatchjdbc.config.querydsl"})
@SpringBootApplication
public class SpringBatchJdbcApplication {

	public static void main(String[] args) {
		System.exit(SpringApplication.exit(SpringApplication.run(SpringBatchJdbcApplication.class, args)));
	}

}
