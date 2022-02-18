package com.luisjrz96.kafka.stream.uppercasesample;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.function.Function;

@SpringBootApplication
public class UppercaseSampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(UppercaseSampleApplication.class, args);
	}

	public static final String DEFAULT_KEY = "DEFAULT_KEY";
	public static final String DEFAULT_VALUE = "DEFAULT_VALUE";
	@Bean
	public Function<KStream<String, String>, KStream<String, String>> process() {
		return input -> input.map((key,value)-> new KeyValue<>(
				(key!=null)? key.toUpperCase() : DEFAULT_KEY,
				(value!=null)? value.toUpperCase() : DEFAULT_VALUE));
	}
}