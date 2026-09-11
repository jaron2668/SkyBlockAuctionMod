package io.github.jaron2668.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.github.jaron2668.skyblocksharedmodels.Flip;
import net.minecraft.ChatFormatting;
import net.minecraft.client.Minecraft;
import net.minecraft.network.chat.ClickEvent;
import net.minecraft.network.chat.MutableComponent;
import net.minecraft.network.chat.Style;
import net.minecraft.network.chat.Component;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;

public final class KafkaFlipConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaFlipConsumer.class);
    private static final String TOPIC_NEW_FLIP = "flipper-newflip";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:19092";
    private static final String CONSUMER_GROUP = "skyblock-auction-mod";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private final BlockingQueue<Flip> messages = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);

    /**
     * Maximum number of flips that can wait in the queue.
     */
    private static final int MAX_QUEUE_SIZE = 500;
    /**
     * Maximum number of chat messages processed during one
     * Minecraft client tick.
     */
    private static final int MAX_MESSAGES_PER_TICK = 20;

    /**
     * KafkaConsumer is only allowed to be accessed by the Kafka consumer thread,
     * except for wakeup() which is thread-safe.
     */
    private volatile KafkaConsumer<String, String> consumer;
    private volatile boolean running;
    private Thread consumerThread;

    public void start() {
        if (running) {
            return;
        }

        running = true;

        consumerThread = new Thread(this::consume, "skyblock-auction-mod-kafka");
        consumerThread.setDaemon(true);
        consumerThread.start();

        LOGGER.info("Kafka flip consumer started. Bootstrap servers: {}", getBootstrapServers());
    }

    /**
     * Stops the Kafka consumer gracefully.
     * <p>
     * KafkaConsumer is not thread-safe, therefore we use wakeup()
     * instead of calling close() from another thread.
     * </p>
     */
    public void stop() {
        if (!running) {
            return;
        }

        LOGGER.info("Stopping Kafka flip consumer...");

        running = false;

        KafkaConsumer<String, String> currentConsumer = consumer;

        if (currentConsumer != null) {
            currentConsumer.wakeup();
        }

        Thread currentThread = consumerThread;

        if (currentThread != null) {
            currentThread.interrupt();
        }
    }

    /**
     * Called from the Minecraft client thread.
     *
     * Processes the flip queue and prints to minecraft chat
     */
    public void drainMessages(Minecraft client) {
        if (client.player == null) {
            return;
        }

        for (int i = 0; i < MAX_MESSAGES_PER_TICK; i++) {
            Flip flip = messages.poll();

            if (flip == null) {
                break;
            }

            client.player.displayClientMessage(createChatMessage(flip), false);
        }
    }

    /**
     * Consumes and processes flip messages from Kafka.
     *
     * <p>
     * The consumer runs while <code>running</code> is <code>true</code>
     * and can be stopped using {@link #stop()}.
     * </p>
     */
    private void consume() {
        Properties properties = createKafkaProperties();

        try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties)) {
            consumer = kafkaConsumer;
            kafkaConsumer.subscribe(Collections.singletonList(TOPIC_NEW_FLIP));
            LOGGER.info("Subscribed to Kafka topic: {}", TOPIC_NEW_FLIP);

            while (running) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
                records.forEach(record -> {
                    parseFlip(record.value());
                });
            }
        } catch (WakeupException exception) {
            /*
             * WakeupException is expected when stop() calls consumer.wakeup().
             */
            if (running) {
                LOGGER.error("Kafka consumer was interrupted unexpectedly.", exception);
            }
        } catch (Exception exception) {
            LOGGER.error("Could not consume Kafka topic {}.", TOPIC_NEW_FLIP, exception);
        } finally {
            consumer = null;
            LOGGER.info("Kafka flip consumer stopped.");
        }
    }

    /**
     * Creates a {@link Flip} by deserializing the Kafka payload.
     * 
     * @param payload the serialized {@link Flip}
     */
    private void parseFlip(String payload) {
        try {
            Flip flip = OBJECT_MAPPER.readValue(payload, Flip.class);
            if (!messages.offer(flip)) {
                LOGGER.warn("Dropping flip event because the message queue is full.");
            }
        } catch (Exception exception) {
            LOGGER.warn("Ignoring malformed flip event from topic {}.", TOPIC_NEW_FLIP, exception);
            LOGGER.debug("Malformed Kafka payload: {}", payload);
        }
    }

    /**
     * Creates a chat message for the given flip.
     * 
     * @param flip the flip for which to create a message
     * @return the created chat message
     */
    private MutableComponent createChatMessage(Flip flip) {
        String command = "/ah view " + flip.getAuctionUuid();

        MutableComponent showText = Component.literal("[show]")
                .setStyle(Style.EMPTY
                        .withColor(ChatFormatting.GREEN)
                        .withUnderlined(true)
                        .withClickEvent(new ClickEvent.RunCommand(command)));

        return Component.literal("[Flip] ")
                .withStyle(ChatFormatting.GOLD)
                .append(Component.literal(flip.getItemDisplayName()).withStyle(ChatFormatting.WHITE))
                .append(Component.literal(" | Estimated profit: ").withStyle(ChatFormatting.GRAY))
                .append(Component.literal(String.format("%,d coins", flip.getEstimatedProfit()))
                        .withStyle(ChatFormatting.GREEN))
                .append(Component.literal(" ").withStyle(ChatFormatting.WHITE))
                .append(showText);
    }

    /**
     * Creates the Kafka consumer properties.
     * 
     * @return the created Kafka consumer properties
     */
    private Properties createKafkaProperties() {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServers());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return properties;
    }

    /**
     * Retrieves the Kafka bootstrap servers configuration.
     * 
     * <p>
     * Checks the system property "skyblock.kafka.bootstrap-servers" first,
     * then the environment variable "SKYBLOCK_KAFKA_BOOTSTRAP_SERVERS",
     * and defaults to "localhost:19092" if neither is set.
     * </p>
     * 
     * @return the bootstrap servers configuration
     */
    private String getBootstrapServers() {
        String configured = System.getProperty("skyblock.kafka.bootstrap-servers");

        if (configured == null || configured.isBlank()) {
            configured = System.getenv("SKYBLOCK_KAFKA_BOOTSTRAP_SERVERS");
        }

        if (configured == null || configured.isBlank()) {
            return DEFAULT_BOOTSTRAP_SERVERS;
        }

        return configured;
    }
}