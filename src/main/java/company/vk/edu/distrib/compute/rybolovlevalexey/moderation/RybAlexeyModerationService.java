package company.vk.edu.distrib.compute.rybolovlevalexey.moderation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import company.vk.edu.distrib.compute.moderation.ModerationService;
import company.vk.edu.distrib.compute.rybolovlevalexey.kafkautils.RybAlexeyKafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RybAlexeyModerationService implements ModerationService {
    private static final String BANNED_WORDS_FILE_PATH = "/moderation/banned_words.txt";
    private static final String TOPIC_LINKS_CREATED = "links.created";
    private static final String TOPIC_LINKS_MODERATED = "links.moderated";
    private static final String TOPIC_LINKS_CREATED_DLT = "links.created.dlt";
    private static final String CONSUMER_GROUP_ID = "rybolovlevalexey-moderation";
    private static final Logger log = LoggerFactory.getLogger(RybAlexeyModerationService.class);

    private final String bootstrapServers;
    private final List<String> bannedWords;
    private final ExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(true);

    @Nullable private KafkaConsumer<String, String> consumer;
    @Nullable private Producer<String, String> producer;

    public RybAlexeyModerationService(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        this.bannedWords = getBannedWords();
        this.executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "moderation-consumer"));
    }

    @Override
    public void start() {
        this.producer = RybAlexeyKafkaUtils.createProducer(bootstrapServers);
        this.consumer = RybAlexeyKafkaUtils.createConsumer(bootstrapServers, CONSUMER_GROUP_ID);
        this.executor.submit(() -> RybAlexeyKafkaUtils.consumeLoop(
            this.consumer, TOPIC_LINKS_CREATED, this.running, this::moderate));
    }

    @Override
    public void stop() {
        this.running.set(false);
        if (this.consumer != null) {
            this.consumer.wakeup();
        }
        this.executor.shutdown();
        if (this.producer != null) {
            this.producer.close();
        }
    }

    private void moderate(ConsumerRecord<String, String> record) {
        LinkCreatedEvent event;
        try {
            event = RybAlexeyKafkaUtils.decodeCreated(record.value());
        } catch (RuntimeException e) {
            log.warn("failed to decode link created event", e);

            var recordDlt = new ProducerRecord<>(TOPIC_LINKS_CREATED_DLT, record.key(), record.value());
            recordDlt.headers().add("error", e.getMessage().getBytes(StandardCharsets.UTF_8));
            producer.send(recordDlt);

            return;
        }
        boolean passed = !hasUrlPathBadWord(event.url());
        String reason = passed ? "" : "contains banned word";
        ModerationResultEvent result = new ModerationResultEvent(event.id(), passed, reason, Instant.now());
        this.producer.send(new ProducerRecord<>(TOPIC_LINKS_MODERATED, result.id(),
            RybAlexeyKafkaUtils.encodeModerated(result)));
    }

    private boolean hasUrlPathBadWord(String url) {
        String decoded = URLDecoder.decode(url, StandardCharsets.UTF_8);
        for (String badWord : bannedWords) {
            if (decoded.contains(badWord)) {
                return true;
            }
        }
        return false;
    }

    private static List<String> getBannedWords() {
        try (InputStream in = RybAlexeyModerationService.class.getResourceAsStream(BANNED_WORDS_FILE_PATH)) {
            if (in == null) {
                throw new IllegalStateException("resource not found " + BANNED_WORDS_FILE_PATH);
            }
            try (BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
                return br.lines().toList();
            }
        } catch (IOException e) {
            throw new IllegalStateException("failed to read " + BANNED_WORDS_FILE_PATH, e);
        }
    }
}
