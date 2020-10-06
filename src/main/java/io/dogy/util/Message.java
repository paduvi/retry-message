package io.dogy.util;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@NoArgsConstructor
@Data
public class Message<T> {

    private String key = UUID.randomUUID().toString();
    private String type;
    private String topic;

    private T payload;
    private Long timestamp = System.currentTimeMillis();
    private Integer times;

    private String serviceName;

    public Message(T payload) {
        this.payload = payload;
    }

    public Message(String type, T payload) {
        this.type = type;
        this.payload = payload;
    }

}
