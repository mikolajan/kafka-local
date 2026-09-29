package shop.dto;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public class OrderModels {

    public static class Item {
        public String name;

        @JsonFormat(shape = JsonFormat.Shape.STRING)
        public Long count;
    }

    public static class Order {
        public String number;
        @JsonProperty("user_id")
        public String userId;
        public String status;
        @JsonProperty("created_at")
        public String createdAt;
        public List<Item> items;
    }

    public static class ItemCount {
        public String name;
        public Long count;

        public ItemCount() {}
        public ItemCount(String name, Long count) {
            this.name = name;
            this.count = count;
        }
    }

    public static class Top5Response {
        @JsonProperty("window_start")
        public String windowStart;

        @JsonProperty("window_end")
        public String windowEnd;

        public List<ItemCount> items;

        public Top5Response() {}
        public Top5Response(String windowStart, String windowEnd, List<ItemCount> items) {
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.items = items;
        }
    }
}
