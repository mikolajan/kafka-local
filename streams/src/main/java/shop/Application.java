package shop;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import shop.streams.NotificationStreamsBuilder;
import shop.streams.TopPendingItemsStream;

import java.util.Properties;

public class Application {
    public static void main(String[] args) {
        System.out.println(">>> Инициализация Kafka Streams приложения...");

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "shop-notifications-stream");

        String bootstrapServers = System.getenv("BOOTSTRAP_SERVERS");
        if (bootstrapServers == null) {
            bootstrapServers = "kafka:9092";
        }
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);

        try {
            StreamsBuilder streamsBuilder = new StreamsBuilder();

            NotificationStreamsBuilder notificationBuilder = new NotificationStreamsBuilder();
            notificationBuilder.buildTopology(streamsBuilder);

            TopPendingItemsStream top5ItemsStream = new TopPendingItemsStream();
            top5ItemsStream.buildTopology(streamsBuilder);

            Topology topology = streamsBuilder.build();

            System.out.println(">>> Схема топологии (Data Flow):");
            System.out.println(topology.describe());

            KafkaStreams streams = new KafkaStreams(topology, config);

            streams.setUncaughtExceptionHandler(exception -> {
                System.err.println("!!! КРИТИЧЕСКАЯ ОШИБКА В ПОТОКЕ ОБРАБОТКИ KAFKA STREAMS:");
                exception.printStackTrace();
                return org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });

            System.out.println(">>> Запуск потоков обработки...");
            streams.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println(">>> Получен сигнал остановки. Закрытие Kafka Streams...");
                streams.close();
                System.out.println(">>> Приложение успешно остановлено.");
            }));

        } catch (Exception e) {
            System.err.println("!!! Критическая ошибка при запуске приложения:");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
