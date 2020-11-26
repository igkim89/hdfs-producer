package com.igkim.kafka.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.igkim.kafka.producer.Parser.HdfsParser;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import com.igkim.kafka.producer.utils.Constants;
import com.igkim.kafka.producer.utils.yaml.HadoopProperties;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Future;

@Component
@RequiredArgsConstructor
public class Producer {
    private final HadoopProperties hadoopProperties;

    @Scheduled(cron = "0/5 * * * * *")
    public void batchComponent() {
        Logger logger = LoggerFactory.getLogger(Producer.class);
        List<Map<String, Object>> result = new ArrayList<>();

        //NameNode Info API 수집
        String namenodeHostname = hadoopProperties.getNameNode().getHostname().get(0);
        String namenodePort = hadoopProperties.getNameNode().getPort();
        String namenodeApiUrl = String.format(Constants.namenodeInfoApi, namenodeHostname, namenodePort);
        JsonObject namenodeInfoJsonObject = apiCall(namenodeApiUrl);
        Map<String, Object> namenodeInfoMap = HdfsParser.namenodeInfoParser(namenodeInfoJsonObject);

        namenodeInfoMap.forEach((key, value) ->
                logger.info("{} : {}", key, value));

        //HDFS Info API 수집
        String hdfsApiUrl = String.format(Constants.hdfsInfoApi, namenodeHostname, namenodePort);
        JsonObject hdfsInfoJsonObject = apiCall(hdfsApiUrl);
        Map<String, Object> hdfsInfoMap = HdfsParser.hdfsInfoParser(hdfsInfoJsonObject);

        hdfsInfoMap.forEach((key, value) ->
                logger.info("{} : {}", key, value));

        //NameNode Heap Memory API 수집
        String namenodeHeapApiUrl = String.format(Constants.namenodeHeapApi, namenodeHostname, namenodePort);
        JsonObject namenodeHeapInfoJsonObject = apiCall(namenodeHeapApiUrl);
        Map<String, Object> namenodeHeapInfoMap = HdfsParser.namenodeHeapInfoParser(namenodeHeapInfoJsonObject);

        namenodeHeapInfoMap.forEach((key, value) ->
                logger.info("{} : {}", key, value));


        //API 취합 후 카프카 전송
        Map<String, Object> kafkaMessage = new HashMap<>();
        kafkaMessage.putAll(namenodeInfoMap);
        kafkaMessage.putAll(hdfsInfoMap);
        kafkaMessage.putAll(namenodeHeapInfoMap);
        kafkaMessage.put("CurrentTime", Timestamp.valueOf(LocalDateTime.now()));

        ObjectMapper mapper = new ObjectMapper();
        try {
//            String strJson = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(kafkaMessage);
            String strJson = mapper.writeValueAsString(kafkaMessage);
            topicSend(strJson);
        } catch (Exception e) {
            logger.error("{}", e);
        }

    }

    public JsonObject apiCall(String apiUrl) {
        JsonObject jsonObject = new JsonObject();
        BufferedReader br = null;

        try {
            URL url = new URL(apiUrl);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.setRequestProperty("Accept", "application/json");
            br = new BufferedReader(new InputStreamReader(con.getInputStream(), "UTF-8"));

            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            JsonParser parser = new JsonParser();
            Object obj = parser.parse(sb.toString());
            jsonObject = (JsonObject)obj;
            System.out.println(jsonObject);

        } catch (Exception e) {
            System.out.println(e);
        }

        return jsonObject;
    }

    public boolean topicSend(String str) {

        KafkaProducer producer = new KafkaProducer(kafkaProp());
        ProducerRecord record = new ProducerRecord(hadoopProperties.getKafka().getTopic(), str);

        Future recordMetadata = producer.send(record);
        producer.close();
        return true;
    }

    public Properties kafkaProp() {
        Properties properties = new Properties();
        String kafkaPort = hadoopProperties.getKafka().getPort();
        String bootstrapServer = null;
        List<String> hostList = hadoopProperties.getKafka().getHostname();
        for (String s : hostList) {
            if (!(bootstrapServer == null)) {
                bootstrapServer = bootstrapServer + "," + s + ":" + kafkaPort;
            } else bootstrapServer = s + ":" + kafkaPort;
        }

        properties.put("bootstrap.servers", bootstrapServer);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "all");

        return properties;
    }

}
