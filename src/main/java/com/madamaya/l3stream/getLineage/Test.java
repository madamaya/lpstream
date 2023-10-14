package com.madamaya.l3stream.getLineage;

public class Test {
    public static void main(String[] args) throws Exception {
        String str = "\"NYCResultTuple{vendorId=2, dropoffLocationId=263, count=3, avgDistance=7.109999999999999, ts=1644915587000}\"";
        System.out.println("str.substring(0, str.length()-1) = " + str.substring(0, str.length()-1));
        System.out.println("str.substring(0, str.length()-1).substring(1) = " + str.substring(0, str.length()-1).substring(1));
        /*
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test5");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        String topicName = "test";
        System.out.println(topicName);
        consumer.subscribe(Arrays.asList(topicName));

        int count = 0;
        boolean run = true;
        // CNFM: このままの実装だと，前の実行時のあまりを受け取る？（要確認）のでcancelを先にやって，N秒入力が来なかったらみたいにする．
        System.out.println("Read");
        while (run) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord record : records) {
                count++;
                String recordValue = (String) record.value();
                System.out.println(recordValue);
            }
        }

         */
    }
}
