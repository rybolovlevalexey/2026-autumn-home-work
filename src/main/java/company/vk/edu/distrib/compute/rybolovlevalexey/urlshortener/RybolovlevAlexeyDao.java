package company.vk.edu.distrib.compute.rybolovlevalexey.urlshortener;
import company.vk.edu.distrib.compute.Dao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

public class RybolovlevAlexeyDao implements Dao<String> {
    private final Map<String, String> storage = new ConcurrentHashMap<>();
    private final RybolovlevAlexeyUrlShortenerUtils serviceUtils;
    private static final Logger log = LoggerFactory.getLogger(RybolovlevAlexeyUrlShortenerServiceFactory.class);

    public RybolovlevAlexeyDao() {
        serviceUtils = new RybolovlevAlexeyUrlShortenerUtils();
    }

    @Override
    public String get(String key) throws NoSuchElementException, IllegalArgumentException, IOException {
        serviceUtils.validateLinkID(key);
        log.info("storage {}", storage.toString());
        final var value = storage.get(key);
        if (value == null){
            throw new NoSuchElementException("no value for key: " + key);
        }
        return value;
    }

    @Override
    public void upsert(String key, String value) throws IllegalArgumentException, IOException {
        serviceUtils.validateLinkID(key);
        storage.put(key, value);
        log.info("storage after upsert {}", storage.toString());
    }

    @Override
    public void delete(String key) throws IllegalArgumentException, IOException {
        log.info("storage before delete {}", storage.toString());
        serviceUtils.validateLinkID(key);
        storage.remove(key);
        log.info("storage after delete {}", storage.toString());
    }

    @Override
    public void close() throws IOException {

    }
}
