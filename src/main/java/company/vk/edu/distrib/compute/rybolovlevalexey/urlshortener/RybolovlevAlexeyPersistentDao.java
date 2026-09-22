package company.vk.edu.distrib.compute.rybolovlevalexey.urlshortener;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import company.vk.edu.distrib.compute.Dao;

public class RybolovlevAlexeyPersistentDao implements Dao<String> {

    private final Map<String, String> storage = new ConcurrentHashMap<>();
    private final Path filePath;

    public RybolovlevAlexeyPersistentDao(Path filePath) throws IOException {
        this.filePath = filePath;
        load();
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        final var value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("no value for key: " + key);
        }
        return value;
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        storage.put(key, value);
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        storage.remove(key);
    }

    @Override
    public void close() throws IOException {
        save();
    }

    private void load() throws IOException {
        if (!Files.exists(filePath)) {
            return;
        }
        final var properties = new Properties();
        try (InputStream in = Files.newInputStream(filePath)) {
            properties.load(in);
        }
        properties.forEach((key, value) -> storage.put((String) key, (String) value));
    }

    private void save() throws IOException {
        final var properties = new Properties();
        storage.forEach(properties::put);
        final var parent = filePath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try (OutputStream out = Files.newOutputStream(filePath)) {
            properties.store(out, null);
        }
    }
}
