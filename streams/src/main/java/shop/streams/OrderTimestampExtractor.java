package shop.streams; 

import shop.dto.OrderModels;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import java.time.Instant;

public class OrderTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        OrderModels.Order order = (OrderModels.Order) record.value();

        if (order != null && order.createdAt != null) {
            try {
                return Instant.parse(order.createdAt).toEpochMilli();
            } catch (Exception e) {
                return partitionTime;
            }
        }
        return partitionTime;
    }
}
