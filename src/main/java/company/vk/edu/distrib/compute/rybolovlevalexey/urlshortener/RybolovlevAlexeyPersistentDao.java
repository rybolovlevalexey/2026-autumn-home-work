package company.vk.edu.distrib.compute.rybolovlevalexey.urlshortener;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import company.vk.edu.distrib.compute.Dao;

public class RybolovlevAlexeyPersistentDao<T> implements Dao<T> {
    private static final String UPSERT_ACTION = "upsert";
    private static final String REMOVE_ACTION = "remove";

    private final Path filePath;
    private final Map<String, T> storage = new ConcurrentHashMap<>();
    private final Serializer<T> serializer;

    public RybolovlevAlexeyPersistentDao(Path filePath, Serializer<T> serializer) throws IOException {
        this.serializer = serializer;

        this.filePath = filePath;

        Files.createDirectories(this.filePath.getParent());
        if (Files.notExists(filePath)) {
            Files.createFile(filePath);
        }

        load();
    }

    public static RybolovlevAlexeyPersistentDao<String> stringBased(Path filePath) throws IOException {
        return new RybolovlevAlexeyPersistentDao<>(filePath, new PropertiesSerializer());
    }

    public static RybolovlevAlexeyPersistentDao<byte[]> bytesBased(Path filePath) throws IOException {
        return new RybolovlevAlexeyPersistentDao<>(filePath, new BytesSerializer());
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
        save(String.format("%s;%s;%s", UPSERT_ACTION, key, serializer.serialize(value)));
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        storage.remove(key);
        save(String.format("%s;%s", REMOVE_ACTION, key));
    }

    @Override
    public void close() throws IOException {
        // nothing to close
    }

    private void save(String action) throws IOException {
        Files.writeString(filePath, action + System.lineSeparator(),
                StandardCharsets.UTF_8, StandardOpenOption.APPEND);
    }

    private void load() throws IOException {
        if (!Files.exists(this.filePath)) {
            return;
        }

        List<String> lines = Files.readAllLines(this.filePath);

        for (String line : lines) {
            String[] terms = line.split(";");

            if (Objects.equals(UPSERT_ACTION, terms[0])) {
                storage.put(terms[1], serializer.deserialize(terms[2]));
            } else if (Objects.equals(REMOVE_ACTION, terms[0])) {
                storage.remove(terms[1]);
            }
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
