package org.msp.kafka;

import java.util.Properties;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerBasic {
    public static void main(String[] args) {
        System.out.println("This is a simple Kafka Producer");

        String BootstrapServers = "127.0.0.1:9092";


        //Create producer properties

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");

        // Create producer

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int count = 0; count < 100; count++) {
            //Create record
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic","Hello World after win 11 migration !");


            // Execute the command every 1 minute, 100 times

            // Execute the command
            producer.send(record);
            try {
                // Sleep for 1 minute
                Thread.sleep(10000); // 60,000 milliseconds = 1 minute
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            // Increment count
            count++;
            System.out.println(count);
        }


        //Send data

        producer.close();




    }
}
