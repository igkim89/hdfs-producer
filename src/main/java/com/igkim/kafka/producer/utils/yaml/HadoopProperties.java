package com.igkim.kafka.producer.utils.yaml;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.List;

@Data
@Component
@ConfigurationProperties(prefix = "hadoop")
public class HadoopProperties {
    private NameNode nameNode;
    private Kafka kafka;

    @Data
    public static class NameNode {
        private boolean ha;
        private List<String> hostname;
        private String port;
    }

    @Data
    public static class Kafka {
        private List<String> hostname;
        private String port;
        private String topic;
    }

}
