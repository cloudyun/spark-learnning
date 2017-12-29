package com.fhzz.zookeeper;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaCluster;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConversions;
 
public class KafkaOffsetExample {
    private static KafkaCluster kafkaCluster = null;
    private static HashMap<String, String> kafkaParam = new HashMap<String, String>();
    private static Broadcast<HashMap<String, String>> kafkaParamBroadcast = null;
    private static scala.collection.immutable.Set<String> immutableTopics = null;
 
    /** * Create the Kafka Stream Directly With Offset in ZK * * @param jssc * SparkStreamContext * @param consumerOffsetsLong * Save the Offset of Kafka Topic * @return */
    private static JavaInputDStream<String> createKafkaDStream(JavaStreamingContext jssc,
            Map<TopicAndPartition, Long> consumerOffsetsLong) {
//        KafkaOffsetExample.log.warn("Create KafkaDriectStream with Offset");
        JavaInputDStream<String> message = KafkaUtils.createDirectStream(jssc, String.class, String.class,
                StringDecoder.class, StringDecoder.class, String.class, kafkaParamBroadcast.getValue(),
                consumerOffsetsLong, new Function<MessageAndMetadata<String, String>, String>() {
                    private static final long serialVersionUID = 1L;
 
                    @Override
                    public String call(MessageAndMetadata<String, String> v1) throws Exception {
                        return v1.message();
                    }
                });
        return message;
    }
 
    private static Map<TopicAndPartition, Long> initConsumerOffset(String topic) {
        Set<String> topicSet = new HashSet<String>();
        topicSet.add(topic);
        scala.collection.mutable.Set<String> mutableTopics = JavaConversions.asScalaSet(topicSet);
        immutableTopics = mutableTopics.toSet();
        scala.collection.immutable.Set<TopicAndPartition> topicAndPartitionSet = kafkaCluster
                .getPartitions(immutableTopics).right().get();
         
        // kafka direct stream 初始化时使用的offset数据
        Map<TopicAndPartition, Long> consumerOffsetsLong = new HashMap<TopicAndPartition, Long>();
        if (kafkaCluster.getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet).isLeft()) {
//            KafkaOffsetExample.log.warn("没有保存offset, 各个partition offset 默认为0");
            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet);
            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                consumerOffsetsLong.put(topicAndPartition, 0L);
            }
        }
        else {
//            KafkaOffsetExample.log.warn("offset已存在, 使用保存的offset");
            scala.collection.immutable.Map<TopicAndPartition, Object> consumerOffsetsTemp = kafkaCluster
                    .getConsumerOffsets(kafkaParam.get("group.id"), topicAndPartitionSet).right().get();
 
            Map<TopicAndPartition, Object> consumerOffsets = JavaConversions.mapAsJavaMap(consumerOffsetsTemp);
            Set<TopicAndPartition> topicAndPartitionSet1 = JavaConversions.setAsJavaSet(topicAndPartitionSet);
 
//            KafkaOffsetExample.log.warn("put data in consumerOffsetsLong");
            for (TopicAndPartition topicAndPartition : topicAndPartitionSet1) {
                Long offset = (Long) consumerOffsets.get(topicAndPartition);
                consumerOffsetsLong.put(topicAndPartition, offset);
            }
        }
        return consumerOffsetsLong;
    }
     
    private static JavaDStream<String> getAndUpdateKafkaOffset(JavaInputDStream<String> message,
            AtomicReference<OffsetRange[]> offsetRanges) {
        JavaDStream<String> javaDStream = message.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            private static final long serialVersionUID = 1L;
            public JavaRDD<String> call(JavaRDD<String> rdd) throws Exception {
                OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                offsetRanges.set(offsets);
//                for (int i = 0; i < offsets.length; i++)
//                    KafkaOffsetExample.log.warn("topic : {}, partitions: {}, fromoffset: {}, untiloffset: {}",
//                            offsets[i].topic(), offsets[i].partition(), offsets[i].fromOffset(),
//                            offsets[i].untilOffset());
                return rdd;
            }
        });
//        KafkaOffsetExample.log.warn("foreachRDD");
        // output
        javaDStream.foreachRDD(new VoidFunction<JavaRDD<String>>() {
            private static final long serialVersionUID = 1L;
 
            public void call(JavaRDD<String> rdd) throws Exception {
                if (rdd.isEmpty()) {
//                    KafkaOffsetExample.log.warn("Empty RDD");
                    return;
                }
                for (OffsetRange o : offsetRanges.get()) {
                    // 封装topic.partition 与 offset对应关系 java Map
                    TopicAndPartition topicAndPartition = new TopicAndPartition(o.topic(), o.partition());
                    Map<TopicAndPartition, Object> topicAndPartitionObjectMap = new HashMap<TopicAndPartition, Object>();
                    topicAndPartitionObjectMap.put(topicAndPartition, o.untilOffset());
 
//                    KafkaOffsetExample.log.warn(
//                            "Topic: " + o.topic() + " partitions: " + o.partition() + " offset : " + o.untilOffset());
 
                    // 转换java map to scala immutable.map
                    scala.collection.mutable.Map<TopicAndPartition, Object> testMap = JavaConversions
                            .mapAsScalaMap(topicAndPartitionObjectMap);
                    scala.collection.immutable.Map<TopicAndPartition, Object> scalatopicAndPartitionObjectMap = testMap
                            .toMap(new Predef.$less$colon$less<Tuple2<TopicAndPartition, Object>, Tuple2<TopicAndPartition, Object>>() {
                                private static final long serialVersionUID = 1L;
 
                                @Override
                                public Tuple2<TopicAndPartition, Object> apply(Tuple2<TopicAndPartition, Object> v1) {
                                    return v1;
                                }
                            });
                    // 更新offset到kafkaCluster
                    kafkaCluster.setConsumerOffsets(kafkaParamBroadcast.getValue().get("group.id"),
                            scalatopicAndPartitionObjectMap);
                }
            }
        });
        return javaDStream;
    }
     
    private static void initKafkaParams() {
        kafkaParam.put("metadata.broker.list", WIFIConfig.BROKER_LIST);
        kafkaParam.put("zookeeper.connect", WIFIConfig.ZK_CONNECT);
        kafkaParam.put("auto.offset.reset", WIFIConfig.AUTO_OFFSET_RESET);
        kafkaParam.put("group.id", WIFIConfig.GROUP_ID);
    }
     
    private static KafkaCluster initKafkaCluster() {
//        KafkaOffsetExample.log.warn("transform java Map to scala immutable.map");
        // transform java Map to scala immutable.map
        scala.collection.mutable.Map<String, String> testMap = JavaConversions.mapAsScalaMap(kafkaParam);
        scala.collection.immutable.Map<String, String> scalaKafkaParam = testMap
                .toMap(new Predef.$less$colon$less<Tuple2<String, String>, Tuple2<String, String>>() {
                    private static final long serialVersionUID = 1L;
 
                    @Override
                    public Tuple2<String, String> apply(Tuple2<String, String> arg0) {
                        return arg0;
                    }
                });
 
        // init KafkaCluster
//        KafkaOffsetExample.log.warn("Init KafkaCluster");
        return new KafkaCluster(scalaKafkaParam);
    }
     
    public static void run() {
        initKafkaParams();
        kafkaCluster = initKafkaCluster();
 
        SparkConf sparkConf = new SparkConf().setMaster("local[4]").setAppName("tachyon-test-consumer");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(5000));
         
        // 得到rdd各个分区对应的offset, 并保存在offsetRanges中
//        KafkaOffsetExample.log.warn("initConsumer Offset");
        Map<TopicAndPartition, Long> consumerOffsetsLong = initConsumerOffset(WIFIConfig.KAFKA_TOPIC);
        kafkaParamBroadcast = jssc.sparkContext().broadcast(kafkaParam);
         
        JavaInputDStream<String> message = createKafkaDStream(jssc, consumerOffsetsLong);
        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();
        JavaDStream<String> javaDStream = getAndUpdateKafkaOffset(message, offsetRanges);
         
        javaDStream.print();
         
        jssc.start();
        jssc.awaitTermination();
    }
     
    public static void main(String[] args) throws Exception {
        String testPath = "E:\\javaCodes\\svn\\SmartCampus\\Trunk\\smartcampus.etl.wifi\\src\\main\\resources\\WifiConfig.yaml";
        WIFIConfig.init(testPath);
//        KafkaOffsetExample.log.warn(WIFIConfig.toStr());
         
        KafkaOffsetExample.run();
    }
}
