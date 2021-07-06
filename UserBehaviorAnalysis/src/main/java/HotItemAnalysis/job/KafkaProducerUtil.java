package HotItemAnalysis.job;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

/**
 * 模拟Kafka生产发送数据
 * @Author: kim
 * @Description:
 * @Date: 9:45 2021/7/6
 * @Version: 1.0
 */
public class KafkaProducerUtil {

    public static void main(String[] args) throws Exception {
        writeToKafka("hotitems");
    }

    public static void writeToKafka(String topic) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 定义KafkaProducer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        BufferedReader reader = new BufferedReader(new FileReader("D:\\WorkSpace\\IDEA\\bixiflink\\UserBehaviorAnalysis\\src\\main\\resources\\UserBehavior.csv"));
        String line;
        while ((line = reader.readLine()) != null) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, line);
            kafkaProducer.send(record);
        }
        kafkaProducer.close();
        reader.close();
    }
}
