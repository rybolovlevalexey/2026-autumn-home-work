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

public class RybolovlevAlexeyPersistentDao<T> implements Dao<T> {

    private final Map<String, T> storage = new ConcurrentHashMap<>();
    private final Path filePath;
    private final Serializer<T> serializer;

    public RybolovlevAlexeyPersistentDao(Path filePath, Serializer<T> serializer) throws IOException {
        this.filePath = filePath;
        this.serializer = serializer;
        load();
        Runtime.getRuntime().addShutdownHook(new Thread(this::closeQuietly));
    }

    public static RybolovlevAlexeyPersistentDao<String> stringBased(Path filePath) throws IOException {
        return new RybolovlevAlexeyPersistentDao<>(filePath, new PropertiesSerializer());
    }

    public static RybolovlevAlexeyPersistentDao<byte[]> bytesBased(Path filePath) throws IOException {
        return new RybolovlevAlexeyPersistentDao<>(filePath, new BytesSerializer());
    }

    private void closeQuietly() {
        try {
            save();
        } catch (IOException e) {
            Thread.currentThread().getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), e);
        }
    }

    @Override
    public T get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        final var value = storage.get(key);
        if (value == null) {
            throw new NoSuchElementException("no value for key: " + key);
        }
        return value;
    }

    @Override
    public void upsert(String key, T value) throws IllegalArgumentException, IOException {
        storage.put(key, value);
        save();
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        storage.remove(key);
        save();
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
        properties.forEach((key, value) -> storage.put((String) key, serializer.deserialize((String) value)));
    }

    private void save() throws IOException {
        final var properties = new Properties();
        storage.forEach((key, value) -> properties.put(key, serializer.serialize(value)));
        final var parent = filePath.getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try (OutputStream out = Files.newOutputStream(filePath)) {
            properties.store(out, null);
        }
    }

    public interface Serializer<T> {
        String serialize(T value);

        T deserialize(String value);
    }

    public static final class PropertiesSerializer implements Serializer<String> {
        @Override
        public String serialize(String value) {
            return value;
        }

        @Override
        public String deserialize(String value) {
            return value;
        }
    }

    public static final class BytesSerializer implements Serializer<byte[]> {
        @Override
        public String serialize(byte[] value) {
            return java.util.Base64.getEncoder().encodeToString(value);
        }

        @Override
        public byte[] deserialize(String value) {
            return java.util.Base64.getDecoder().decode(value);
        }
    }
}
