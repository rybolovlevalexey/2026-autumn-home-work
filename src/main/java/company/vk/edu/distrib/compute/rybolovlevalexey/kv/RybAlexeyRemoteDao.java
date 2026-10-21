package company.vk.edu.distrib.compute.rybolovlevalexey.kv;

import company.vk.edu.distrib.compute.Dao;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.NoSuchElementException;

public class RybAlexeyRemoteDao implements Dao<String> {

    private static final String PATH_ENTITY_V0 = "/v0/entity";

    private final HttpClient httpClient;
    private final int port;

    public RybAlexeyRemoteDao(HttpClient httpClient, int port) {
        this.httpClient = httpClient;
        this.port = port;
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        final var request = HttpRequest.newBuilder(uri(key)).GET().build();
        final var response = send(request);
        return switch (response.statusCode()) {
            case 200 -> response.body();
            case 404 -> throw new NoSuchElementException("No entity for key " + key);
            case 422 -> throw new IllegalArgumentException("Invalid key " + key);
            default -> throw new IOException("KV returned " + response.statusCode());
        };
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        final var request = HttpRequest.newBuilder(uri(key))
            .PUT(HttpRequest.BodyPublishers.ofString(value, StandardCharsets.UTF_8))
            .build();
        final var response = send(request);
        switch (response.statusCode()) {
            case 201 -> { }
            case 422 -> throw new IllegalArgumentException("Invalid key " + key);
            default -> throw new IOException("KV returned " + response.statusCode());
        }
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        final var request = HttpRequest.newBuilder(uri(key)).DELETE().build();
        final var response = send(request);
        switch (response.statusCode()) {
            case 202 -> { }
            case 422 -> throw new IllegalArgumentException("Invalid key " + key);
            default -> throw new IOException("KV returned " + response.statusCode());
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }

    private HttpResponse<String> send(HttpRequest request) throws IOException {
        try {
            return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while sending request to KV", e);
        }
    }

    private URI uri(String key) {
        return URI.create(String.format("http://localhost:%d%s?id=%s", port, PATH_ENTITY_V0, key));
    }
}
