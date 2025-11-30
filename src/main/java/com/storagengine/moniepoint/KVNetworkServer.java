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
    private final TcpServer tcpServer;
    private reactor.netty.DisposableServer server;

    public KVNetworkServer(KeyValueStore store) {
        this.store = store;
        this.tcpServer = TcpServer.create()
                .port(8080)
                .wiretap(true);
    }

    @PostConstruct
    public void start() {
        server = tcpServer.handle((inbound, outbound) ->
                inbound.receive()
                        .asString(StandardCharsets.UTF_8)
                        .flatMap(this::handleMessage)
                        .flatMap(response -> outbound.sendString(Mono.just(response)).then())
                        .onErrorResume(err -> outbound.sendString(Mono.just("ERROR: " + err.getMessage())).then())
        ).bindNow();
        System.out.println("TCP server started on port 8080");
    }

    private Mono<String> handleMessage(String message) {
        // Simple protocol example: "PUT key=value" | "GET key" etc.
        try {
            var parts = message.strip().split(" ", 2);
            var cmd = parts[0].toUpperCase();
            if ("PUT".equals(cmd)) {
                var kv = parts[1].split("=", 2);
                store.put(kv[0], kv[1].getBytes(StandardCharsets.UTF_8));
                return Mono.just("OK");
            } else if ("GET".equals(cmd)) {
                var valOpt = store.read(parts[1]);
                if (valOpt.isPresent())
                    return Mono.just(new String(valOpt.get(), StandardCharsets.UTF_8));
                else
                    return Mono.just("NOT_FOUND");
            } else {
                return Mono.just("UNKNOWN_COMMAND");
            }
        } catch (Exception e) {
            return Mono.just("ERROR: invalid input - " + e.getMessage());
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
