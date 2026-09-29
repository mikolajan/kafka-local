package shop.streams;

import shop.dto.OrderModels;
import shop.dto.CustomJsonSerde;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.TimestampExtractor;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class TopPendingItemsStream {

    private static final String ORDERS_TOPIC = "shop.orders";
    private static final String TOP_TOPIC = "shop.top-pending-items";
    private static final int ITEMS_COUNT = 3;
    private static final Duration WINDOW_SIZE = Duration.ofMinutes(1);
    private static final ObjectMapper mapper = new ObjectMapper();

    // Кастомный экстрактор времени, работающий напрямую с сырой JSON-строкой
    public static class SafeJsonTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            String json = (String) record.value();
            if (json != null) {
                try {
                    // Быстрое извлечение поля created_at без полной десериализации всего объекта
                    var node = mapper.readTree(json);
                    String createdAt = node.path("created_at").asText();
                    if (!createdAt.isEmpty()) {
                        return Instant.parse(createdAt).toEpochMilli();
                    }
                } catch (Exception e) {
                    // В случае ошибки парсинга времени используем системное время записи
                    return partitionTime;
                }
            }
            return partitionTime;
        }
    }

    public void buildTopology(StreamsBuilder builder) {
        TimeWindows timeWindow = TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE);

        // ИСПРАВЛЕНО: Читаем как String, но ОБЯЗАТЕЛЬНО передаем кастомный экстрактор событийного времени
        KStream<String, String> rawOrderStream = builder.stream(ORDERS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withTimestampExtractor(new SafeJsonTimestampExtractor()));

        KTable<Windowed<String>, Long> itemCounts = rawOrderStream
            .flatMap((key, orderJson) -> {
                List<KeyValue<String, Long>> result = new ArrayList<>();
                try {
                    OrderModels.Order order = mapper.readValue(orderJson, OrderModels.Order.class);

                    if (order != null && "pending".equalsIgnoreCase(order.status) && order.items != null) {
                        for (OrderModels.Item item : order.items) {
                            if (item.name != null && item.count != null) {
                                result.add(new KeyValue<>(item.name, item.count));
                            }
                        }
                    }
                } catch (Exception e) {
                    System.err.println("!!! Ошибка парсинга JSON заказа в Топ-стриме: " + e.getMessage());
                }
                return result;
            })
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
            .windowedBy(timeWindow)
            .reduce(Long::sum, Materialized.with(Serdes.String(), Serdes.Long()));

        KStream<Windowed<String>, Long> suppressedCountsStream = itemCounts
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .toStream();

        CustomJsonSerde<ArrayList> rawListSerde = new CustomJsonSerde<>(ArrayList.class);

        // 2. Перегруппировываем по началу окна для вычисления Топ-3
        KTable<Long, ArrayList> topItemsPerWindow = suppressedCountsStream
            .map((windowedItem, count) -> new KeyValue<>(
                    windowedItem.window().start(),
                    new OrderModels.ItemCount(windowedItem.key(), count)
            ))
            .groupBy((windowStart, itemCountObj) -> windowStart,
                     Grouped.with(Serdes.Long(), new CustomJsonSerde<>(OrderModels.ItemCount.class)))
            .aggregate(
                () -> new ArrayList<>(),
                (windowStart, newItemCount, currentRawList) -> {
                    List<OrderModels.ItemCount> typedList = new ArrayList<>();
                    for (Object obj : currentRawList) {
                        typedList.add(mapper.convertValue(obj, OrderModels.ItemCount.class));
                    }

                    typedList.removeIf(i -> i.name.equals(newItemCount.name));
                    typedList.add(newItemCount);

                    List<OrderModels.ItemCount> sorted = typedList.stream()
                            .sorted((a, b) -> b.count.compareTo(a.count))
                            .limit(ITEMS_COUNT)
                            .collect(Collectors.toList());

                    return new ArrayList<>(sorted);
                },
                Materialized.with(Serdes.Long(), rawListSerde)
            );

        // 3. Формируем финальный payload
        topItemsPerWindow.toStream()
            .map((windowStartMs, rawList) -> {
                long endMs = windowStartMs + WINDOW_SIZE.toMillis();
                String startTimeIso = Instant.ofEpochMilli(windowStartMs).toString();
                String endTimeIso = Instant.ofEpochMilli(endMs).toString();

                List<OrderModels.ItemCount> typedList = new ArrayList<>();
                for (Object obj : rawList) {
                    typedList.add(mapper.convertValue(obj, OrderModels.ItemCount.class));
                }

                OrderModels.Top5Response response = new OrderModels.Top5Response(startTimeIso, endTimeIso, typedList);
                return new KeyValue<>(startTimeIso, response);
            })
            .to(TOP_TOPIC, Produced.with(Serdes.String(), new CustomJsonSerde<>(OrderModels.Top5Response.class)));
    }
}
