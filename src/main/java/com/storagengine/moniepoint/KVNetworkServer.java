package com.storagengine.moniepoint;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.netty.tcp.TcpServer;

import java.nio.charset.StandardCharsets;

@Service
public class KVNetworkServer {
    private final KeyValueStore store;
    private reactor.netty.DisposableServer server;

    public KVNetworkServer(KeyValueStore store) {
        this.store = store;
    }

    @PostConstruct
    public void startServer() {
        server = TcpServer.create()
                .port(8080)
                .wiretap(true)
                .handle((inbound, outbound) ->
                        inbound.receive()
                                .asString(StandardCharsets.UTF_8)
                                .flatMap(this::processCommand)
                                .flatMap(response -> outbound.sendString(Mono.just(response)).then())
                                .onErrorResume(err -> outbound.sendString(Mono.just("ERROR: " + err.getMessage())).then())
                ).bindNow();

        System.out.println("TCP server started on port 8080");
    }

    private Mono<String> processCommand(String message) {
        try {
            var parts = message.strip().split(" ", 2);
            var cmd = parts[0].toUpperCase();

            switch (cmd) {
                case "PUT" -> {
                    var kv = parts[1].split("=", 2);
                    if (kv.length < 2) return Mono.just("ERROR: Invalid put command, expected 'PUT key=value'");
                    store.put(kv[0], kv[1].getBytes(StandardCharsets.UTF_8));
                    return Mono.just("OK");
                }
                case "GET" -> {
                    var val = store.read(parts[1]);
                    return Mono.just(val.map(v -> new String(v, StandardCharsets.UTF_8)).orElse("NOT_FOUND"));
                }
                case "DELETE" -> {
                    store.delete(parts[1]);
                    return Mono.just("OK");
                }
                case "BATCHPUT" -> {
                    // Implement this if needed - can parse JSON or another protocol
                    return Mono.just("BATCHPUT command not implemented yet");
                }
                case "READKEYRANGE" -> {
                    // Implement range read parsing and response
                    return Mono.just("READKEYRANGE command not implemented yet");
                }
                default -> {
                    return Mono.just("UNKNOWN_COMMAND");
                }
            }
        } catch (Exception e) {
            return Mono.just("ERROR: " + e.getMessage());
        }
    }

    @PreDestroy
    public void stop() {
        if (server != null) {
            server.disposeNow();
            System.out.println("TCP server stopped");
        }
    }
}
