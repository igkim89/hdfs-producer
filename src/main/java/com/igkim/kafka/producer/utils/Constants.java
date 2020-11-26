package com.igkim.kafka.producer.utils;

public class Constants {

    public static String namenodeInfoApi = "http://%s:%s/jmx?qry=Hadoop:service=NameNode,name=NameNodeInfo";
    public static String hdfsInfoApi = "http://%s:%s/jmx?qry=Hadoop:service=NameNode,name=FSNamesystem";
    public static String namenodeHeapApi = "http://%s:%s/jmx?qry=Hadoop:service=NameNode,name=JvmMetrics";

}
