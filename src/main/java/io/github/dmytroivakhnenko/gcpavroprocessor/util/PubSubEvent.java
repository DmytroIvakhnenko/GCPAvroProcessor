package io.github.dmytroivakhnenko.gcpavroprocessor.util;

import lombok.Data;

/**
 * Body.Message is the payload of a Pub/Sub event. Please refer to the docs for
 * additional information regarding Pub/Sub events.
 */
@Data
public class PubSubEvent {
    private Message message;

    @Data
    public class Message {
        private String messageId;
        private String publishTime;
        private String data;
    }
}