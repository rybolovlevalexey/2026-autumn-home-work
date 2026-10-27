package company.vk.edu.distrib.compute.rybolovlevalexey.kv;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public record ErrorHandler(HttpHandler delegate) implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
        try {
            delegate.handle(exchange);
        } catch (IllegalArgumentException e) {
            final var message = e.getMessage();
            final var body = message.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(422, body.length);
            exchange.getResponseBody().write(body);
        } catch (Exception e) {
            final var message = e.getMessage();
            final var body = message.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(500, body.length);
            exchange.getResponseBody().write(body);
        }
        exchange.close();
    }
}
