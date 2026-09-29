package shop.streams;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Joined;

public class NotificationStreamsBuilder {

    private static final String ORDERS_TOPIC = "shop.orders";
    private static final String CUSTOMERS_TOPIC = "shop.customers";
    private static final String NOTIFICATIONS_TOPIC = "shop.notifications";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public void buildTopology(StreamsBuilder streamsBuilder) {

        KTable<String, String> customersTable = streamsBuilder.table(
                CUSTOMERS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, String> originalOrdersStream = streamsBuilder.stream(
                ORDERS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        KStream<String, String> rekeyedOrdersStream = originalOrdersStream.selectKey(
                (orderId, orderJson) -> cleanKey(parseUserIdFromJson(orderJson))
        );

        KStream<String, String> notificationsStream = rekeyedOrdersStream.leftJoin(
                customersTable,
                (orderJson, customerJson) -> {
                    try {
                        var orderNode = objectMapper.readTree(orderJson);
                        String orderNumber = orderNode.path("number").asText("unknown");
                        String orderStatus = orderNode.path("status").asText("unknown");

                        ObjectNode resultNode;

                        if (customerJson != null && !customerJson.isBlank()) {
                            resultNode = (ObjectNode) objectMapper.readTree(customerJson);
                        } else {
                            resultNode = objectMapper.createObjectNode();
                            resultNode.put("user_id", orderNode.path("user_id").asText("unknown"));
                        }

                        resultNode.put("order_number", orderNumber);
                        resultNode.put("order_status", orderStatus);

                        return objectMapper.writeValueAsString(resultNode);
                    } catch (Exception e) {
                        System.err.println("!!! Ошибка объединения JSON в NotificationStream: " + e.getMessage());
                        return "{\"error\": \"Invalid JSON structures\"}";
                    }
                },
                Joined.with(Serdes.String(), Serdes.String(), Serdes.String())
        );

        KStream<String, String> finalNotificationsStream = notificationsStream.selectKey(
                (currentKey, notificationJson) -> {
                    try {
                        var node = objectMapper.readTree(notificationJson);
                        return node.path("order_number").asText("unknown_order");
                    } catch (Exception e) {
                        return "unknown_order";
                    }
                }
        );

        finalNotificationsStream.to(NOTIFICATIONS_TOPIC, Produced.with(Serdes.String(), Serdes.String()));
    }

    private String parseUserIdFromJson(String json) {
        try {
            if (json != null && json.contains("\"user_id\":\"")) {
                int start = json.indexOf("\"user_id\":\"") + 11;
                int end = json.indexOf("\"", start);
                return json.substring(start, end);
            }
            return "unknown_user";
        } catch (Exception e) {
            return "unknown_user";
        }
    }

    private String cleanKey(String key) {
        if (key == null) return null;
        return key.replace("\"", "").trim();
    }
}
