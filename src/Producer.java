import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class Producer {
    public static void main(String[] args) {
        KafkaProducer producer;
        String broker = "localhost:9092";
        String topic = "kseb";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        producer = new KafkaProducer(props);

        Random random = new Random();
        int low = 1;
        int high = 10;
        int randomNumber;


        while (true){
            try {

                randomNumber = random.nextInt(high + low) + low;
                String jsonFile = String.format("{'userid':2,'unit':"+randomNumber+"}");
                System.out.println(randomNumber);
                System.out.println(jsonFile);
                producer.send(new ProducerRecord(topic,String.valueOf(jsonFile)));


                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

        }



    }
}
