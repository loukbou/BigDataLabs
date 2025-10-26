package edu.ensias.kafka.kafka_app;

import java.util.Properties;
import java.util.Scanner;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class WordProducer {

    public static void main(String[] args) {

        String topicName = "connect-test";

        // Configuration du producteur Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        Scanner scanner = new Scanner(System.in);

        System.out.println(" Tapez du texte (Ctrl+C pour quitter):");

        while (true) {
            String line = scanner.nextLine();
            producer.send(new ProducerRecord<>(topicName, null, line));
            System.out.println("Envoyé : " + line);
        }
    }
}
