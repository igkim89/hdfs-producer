package com.igkim.kafka.producer.Parser;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.igkim.kafka.producer.utils.Converter;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class HdfsParser {

    /**
     * NameNode 총 용량, 사용 중 용량, 잔여 용량, 사용률(%) 등 수집
     * @param jsonObject
     * @return
     */
    public static Map<String, Object> namenodeInfoParser(JsonObject jsonObject) {
        Map<String, Object> result = new HashMap<>();

        JsonArray jsonBeans = jsonObject.getAsJsonArray("beans");
        JsonObject jsonBean = (JsonObject) jsonBeans.get(0);

        long totalSize = Long.parseLong(jsonBean.get("Total").toString());
        String convertTotalSize = Converter.humanReadableByteConverter(totalSize);
        long usedSize = Long.parseLong(jsonBean.get("Used").toString());
        String convertUsedSize = Converter.humanReadableByteConverter(usedSize);
        long freeSize = Long.parseLong(jsonBean.get("Free").toString());
        String convertFreeSize = Converter.humanReadableByteConverter(freeSize);
        Double percentUsed = Double.parseDouble(jsonBean.get("PercentUsed").toString());
        Timestamp startTime = new Timestamp(Long.parseLong(jsonBean.get("NNStartedTimeInMillis").toString()));
        boolean safeMode = false;
        if (jsonBean.get("Safemode").toString().length() > 3) {
            safeMode = true;
        }

        result.put("TotalSize", totalSize);
        result.put("ConvertedTotalSize", convertTotalSize);
        result.put("UsedSize", usedSize);
        result.put("ConvertedUsedSize", convertUsedSize);
        result.put("FreeSize", freeSize);
        result.put("ConvertedFreeSize", convertFreeSize);
        result.put("PercentUsed", percentUsed);
        result.put("StartedTime", startTime);
        result.put("SafeMode", safeMode);

        return result;
    }

    /**
     * HDFS File, Block, Live DataNode, Dead DataNode 개수 등 수집
     * @param jsonObject
     * @return
     */
    public static Map<String, Object> hdfsInfoParser(JsonObject jsonObject) {
        Map<String, Object> result = new HashMap<>();

        JsonArray jsonBeans = jsonObject.getAsJsonArray("beans");
        JsonObject jsonBean = (JsonObject) jsonBeans.get(0);

        long fileCount = Long.parseLong(jsonBean.get("FilesTotal").toString());
        long blockCount = Long.parseLong(jsonBean.get("BlocksTotal").toString());
        long liveDataNodeCount = Long.parseLong(jsonBean.get("NumLiveDataNodes").toString());
        long deadDataNodeCount = Long.parseLong(jsonBean.get("NumDeadDataNodes").toString());
        long underReplicateBlockCount = Long.parseLong(jsonBean.get("UnderReplicatedBlocks").toString());
        long missingReplicateBlockCount = Long.parseLong(jsonBean.get("MissingReplicatedBlocks").toString());
        long curruptBlockCount = Long.parseLong(jsonBean.get("CorruptBlocks").toString());

        result.put("FileCount", fileCount);
        result.put("BlockCount", blockCount);
        result.put("LiveDataNodeCount", liveDataNodeCount);
        result.put("DeadDataNodeCount", deadDataNodeCount);
        result.put("UnderReplicateBlockCount", underReplicateBlockCount);
        result.put("MissingReplicateBlockCount", missingReplicateBlockCount);
        result.put("CurruptBlockCount", curruptBlockCount);

        return result;
    }

    public static Map<String, Object> namenodeHeapInfoParser(JsonObject jsonObject) {
        Map<String, Object> result = new HashMap<>();

        JsonArray jsonBeans = jsonObject.getAsJsonArray("beans");
        JsonObject jsonBean = (JsonObject) jsonBeans.get(0);

        Double nameNodeHeapTotal = Double.parseDouble(jsonBean.get("MemHeapMaxM").toString());
        Double nameNodeHeapUsed = Double.parseDouble(jsonBean.get("MemHeapUsedM").toString());

        result.put("NameNodeHeapTotalM", nameNodeHeapTotal);
        result.put("NameNodeHeapUsedM", nameNodeHeapUsed);

        return result;
    }
}
