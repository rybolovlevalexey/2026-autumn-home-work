package company.vk.edu.distrib.compute.rybolovlevalexey.kafkautils;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import company.vk.edu.distrib.compute.moderation.LinkCreatedEvent;
import company.vk.edu.distrib.compute.moderation.ModerationResultEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class RybAlexeyKafkaUtils {
    private static final char SEP = '\u0001';
    private static final int CREATED_PARTS = 3;
    private static final int MODERATED_PARTS = 4;
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private RybAlexeyKafkaUtils() {
    }

    public static Producer<String, String> createProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 2000);
        return new KafkaProducer<>(props);
    }

    public static KafkaConsumer<String, String> createConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    public static void consumeLoop(KafkaConsumer<String, String> consumer, String topic,
        AtomicBoolean running, Consumer<ConsumerRecord<String, String>> handler) {
        try (KafkaConsumer<String, String> resource = consumer) {
            resource.subscribe(List.of(topic));
            while (running.get()) {
                boolean handled = false;
                for (ConsumerRecord<String, String> record : resource.poll(POLL_TIMEOUT)) {
                    handler.accept(record);
                    handled = true;
                }
                if (handled) {
                    resource.commitSync();
                }
            }
            resource.commitSync();
        }
    }

    public static String encodeCreated(LinkCreatedEvent event) {
        return event.id() + SEP + event.url() + SEP + event.createdAt().toEpochMilli();
    }

    public static LinkCreatedEvent decodeCreated(String value) {
        String[] parts = value.split(String.valueOf(SEP), -1);
        if (parts.length != CREATED_PARTS) {
            throw new IllegalArgumentException("malformed LinkCreatedEvent: " + value);
        }
        return new LinkCreatedEvent(parts[0], parts[1], Instant.ofEpochMilli(Long.parseLong(parts[2])));
    }

    public static String encodeModerated(ModerationResultEvent event) {
        return event.id() + SEP + event.passed() + SEP + event.reason() + SEP + event.moderatedAt().toEpochMilli();
    }

    public static ModerationResultEvent decodeModerated(String value) {
        String[] parts = value.split(String.valueOf(SEP), -1);
        if (parts.length != MODERATED_PARTS) {
            throw new IllegalArgumentException("malformed ModerationResultEvent: " + value);
        }
        return new ModerationResultEvent(parts[0], Boolean.parseBoolean(parts[1]), parts[2],
            Instant.ofEpochMilli(Long.parseLong(parts[3])));
    }
}
