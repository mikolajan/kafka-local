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

    public static class SafeJsonTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
            String json = (String) record.value();
            if (json != null) {
                try {
                    var node = mapper.readTree(json);
                    String createdAt = node.path("created_at").asText();
                    if (!createdAt.isEmpty()) {
                        return Instant.parse(createdAt).toEpochMilli();
                    }
                } catch (Exception e) {
                    return partitionTime;
                }
            }
            return partitionTime;
        }
    }

    public void buildTopology(StreamsBuilder builder) {
        TimeWindows timeWindow = TimeWindows.ofSizeWithNoGrace(WINDOW_SIZE);

        KStream<String, String> rawOrderStream = builder.stream(ORDERS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withTimestampExtractor(new SafeJsonTimestampExtractor()));

        // 1. Считаем количество товаров в окнах
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
                    System.err.println("!!! Ошибка парсинга JSON: " + e.getMessage());
                }
                return result;
            })
            .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
            .windowedBy(timeWindow)
            .reduce(Long::sum, Materialized.with(Serdes.String(), Serdes.Long()));

        // Первый suppress: выпускает только финальные суммы по каждому товару при закрытии окна
        KStream<Windowed<String>, Long> suppressedCountsStream = itemCounts
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .toStream();

        CustomJsonSerde<ArrayList> rawListSerde = new CustomJsonSerde<>(ArrayList.class);

        // 2. Группируем по пересобранному Windowed-ключу
        KTable<Windowed<String>, ArrayList> topItemsPerWindow = suppressedCountsStream
            .map((windowedItem, count) -> {
                // ВАЖНО: сохраняем сам объект окна (TimeWindow), но подменяем внутренний текстовый ключ на общий "ALL_ITEMS"
                Windowed<String> groupKey = new Windowed<>("ALL_ITEMS", windowedItem.window());
                return new KeyValue<>(
                        groupKey,
                        new OrderModels.ItemCount(windowedItem.key(), count)
                );
            })
            // Группируем используя Windowed Serdes, чтобы сохранить метаданные окна для следующего suppress
            .groupBy(
                (windowedKey, itemCount) -> windowedKey,
                Grouped.with(new WindowedSerdes.TimeWindowedSerde<>(Serdes.String(), WINDOW_SIZE.toMillis()), new CustomJsonSerde<>(OrderModels.ItemCount.class))
            )
            // Агрегируем БЕЗ вызова .windowedBy(), так как ключ САМ ПО СЕБЕ уже является окном
            .aggregate(
                () -> new ArrayList<>(),
                (windowedKey, newItemCount, currentRawList) -> {
                    List<OrderModels.ItemCount> typedList = new ArrayList<>();
                    if (currentRawList != null) {
                        for (Object obj : currentRawList) {
                            typedList.add(mapper.convertValue(obj, OrderModels.ItemCount.class));
                        }
                    }

                    typedList.removeIf(i -> i.name.equals(newItemCount.name));
                    typedList.add(newItemCount);

                    List<OrderModels.ItemCount> sorted = typedList.stream()
                            .sorted((a, b) -> b.count.compareTo(a.count))
                            .limit(ITEMS_COUNT)
                            .collect(Collectors.toList());

                    return new ArrayList<>(sorted);
                },
                Materialized.with(new WindowedSerdes.TimeWindowedSerde<>(Serdes.String(), WINDOW_SIZE.toMillis()), rawListSerde)
            );

        // 3. Второй ВАЖНЫЙ suppress: теперь он видит структуру Windowed-ключа
        // и задерживает отправку ТОПА до тех пор, пока окно окончательно не закроется!
        topItemsPerWindow
            .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .toStream()
            .map((windowedKey, rawList) -> {
                long windowStartMs = windowedKey.window().start();
                long endMs = windowedKey.window().end();
                String startTimeIso = Instant.ofEpochMilli(windowStartMs).toString();
                String endTimeIso = Instant.ofEpochMilli(endMs).toString();

                List<OrderModels.ItemCount> typedList = new ArrayList<>();
                for (Object obj : rawList) {
                    typedList.add(mapper.convertValue(obj, OrderModels.ItemCount.class));
                }

                OrderModels.Top5Response response = new OrderModels.Top5Response(startTimeIso, endTimeIso, typedList);
                // В качестве ключа топика возвращаем ISO-строку начала окна
                return new KeyValue<>(startTimeIso, response);
            })
            .to(TOP_TOPIC, Produced.with(Serdes.String(), new CustomJsonSerde<>(OrderModels.Top5Response.class)));
    }
}
