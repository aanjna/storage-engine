package com.storagengine.moniepoint;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class MoniepointApplication {

	@Bean
	public KeyValueStore keyValueStore() throws Exception {
		WriteAheadLog wal = new FileWriteAheadLog("data/wal.log");
		return new StorageEngine(wal);
	}

	public static void main(String[] args) {
		SpringApplication.run(MoniepointApplication.class, args);
	}

}
