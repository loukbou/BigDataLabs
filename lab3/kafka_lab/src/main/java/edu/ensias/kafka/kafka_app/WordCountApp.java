package edu.ensias.kafka.kafka_app;

import java.util.Properties;
import java.util.Arrays;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class WordCountApp {

    public static void main(String[] args) throws Exception {

        String topicName = "connect-test";

        // Configurer le consumer Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "wordcount-group");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Créer le consumer Kafka
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Souscrire au topic
        consumer.subscribe(Arrays.asList(topicName));

        System.out.println("Souscription réussie au topic : " + topicName);
        System.out.println("En attente de messages...");

        // Dictionnaire pour compter les mots
        Map<String, Integer> wordCountMap = new HashMap<>();

        // Boucle infinie d’écoute
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                String message = record.value();
                String[] words = message.split("\\s+");

                for (String word : words) {
                    if (!word.isEmpty()) {
                        word = word.toLowerCase();
                        wordCountMap.put(word, wordCountMap.getOrDefault(word, 0) + 1);
                    }
                }

                // Afficher le compteur actualisé
                System.out.println("\n--- Word Count ---");
                for (Map.Entry<String, Integer> entry : wordCountMap.entrySet()) {
                    System.out.println(entry.getKey() + " : " + entry.getValue());
                }
            }
        }
    }
}
