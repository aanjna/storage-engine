package com.storagengine.moniepoint;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@Configuration
public class KVStoreConfig {

    @Bean
    public KeyValueStore keyValueStore() throws IOException {
        Path storageFolder = Path.of("./storedata");
        Files.createDirectories(storageFolder);
        return new StorageEngine(storageFolder);
    }
}
