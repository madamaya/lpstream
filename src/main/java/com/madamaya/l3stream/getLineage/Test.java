package com.madamaya.l3stream.getLineage;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class Test {
    public static void main(String[] args) throws Exception {
        for (int i = 0; i < 10; i++) {
            System.out.print("\rcount=" + i);
            Thread.sleep(1000);
        }
        /*
        String str = "{\"OUT\":\"NexmarkJoinedTuple{auctionId=7166, bidder=4001, price=1271, channel='Apple', url='https://www.nexmark.com/enb/_jpd/_ptj/item.htm?query=1', bid_dateTime=1696278695312, bid_extra=" + "PJLT\\ZWOO[]fnraiomwcxyrvhzxxwK^\\UaMQaVOI[aHXaKKUKNZLHnz\", itemName='ejnxbgevtelln', desc='bdmwpffu xguuubxcfxdzsrd efpzmbufh ubicfivy hnfnyua ip', initBid=1231321, reserve=1231493, auction_dateTime=1696278695307, expires=1696278695327, seller=3000, category=10, auction_extra='bdnwhpghmuelaOLVXWrpmflbqfjjhtqhdzwp[`IJI_I_]RSZokneojokvima_ZJH^MHZWITRRNaMIHqsfwjnV]LOLJVKKWK_rhbbcelylgnoqwstfbfaebxoqdevjj^UaPVQdhldpgMHaZI]sfkmzkYMIJM`hwplhdeayxifwbhoejQXMVQIhxafzzfpoykxKQ\\KXO`_MRLXwrcwdfpsazngMYSNM_smhrfupwhaakYM[_NJkyrwtwPX^OUPTUNTLM`WK]VI`aJ_XZMSK[]TbxjusxS[MXaTlmhcywW`O\\^UqjfauyTI^TPNLLMV\\I_PHP\\MfvkicynfndcjLNI]Y`pjhwomqskwxeHHNQWWIVKTKImvdlfcfbptha\\X_SNTX^^U'}\",\"CPID\":\"1\",\"TS\":\"1696278695312\"}\n";
        JsonNode jsonNode = new ObjectMapper().readTree(str);
        System.out.println(jsonNode);
         */
        /*
        String str = "\"NYCResultTuple{vendorId=2, dropoffLocationId=263, count=3, avgDistance=7.109999999999999, ts=1644915587000}\"";
        System.out.println("str.substring(0, str.length()-1) = " + str.substring(0, str.length()-1));
        System.out.println("str.substring(0, str.length()-1).substring(1) = " + str.substring(0, str.length()-1).substring(1));
         */
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
