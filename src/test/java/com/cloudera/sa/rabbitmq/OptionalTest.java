package com.cloudera.sa.rabbitmq;

import static org.junit.Assert.*;

import org.junit.Test;

import com.cloudera.sa.rabbitmq.flume.source.RabbitMqSourceConfiguration;
import com.google.common.base.Optional;

public class OptionalTest {

	@Test
	public void test() {
		Optional<String> userName = Optional.fromNullable("kaufman");
		assertTrue(userName.isPresent());
		
		userName = Optional.fromNullable("");
		assertTrue(userName.isPresent());

		userName = Optional.fromNullable(null);
		assertFalse(userName.isPresent());
	}

}
