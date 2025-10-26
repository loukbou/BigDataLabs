package edu.ensias.kafka.kafka_app;
import java.util.Properties;
import java.util.Arrays;
import java.time.Duration;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EventConsumer {

    public static void main(String[] args) throws Exception {

        // Vérifier que le topic est fourni comme argument
        if (args.length == 0) {
            System.out.println("Veuillez entrer le nom du topic en argument.");
            return;
        }

        // Récupérer le nom du topic
        String topicName = args[0];

        // Configurer le consumer Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Adresse du broker Kafka
        props.put("group.id", "test");                    // Identifiant du groupe de consommateurs
        props.put("enable.auto.commit", "true");          // Validation automatique de l’offset
        props.put("auto.commit.interval.ms", "1000");     // Fréquence de commit (en ms)
        props.put("session.timeout.ms", "30000");         // Timeout de session
        props.put("key.deserializer", 
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", 
                  "org.apache.kafka.common.serialization.StringDeserializer");

        // Créer le consommateur Kafka
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Souscrire au topic
        consumer.subscribe(Arrays.asList(topicName));

        System.out.println("Souscription réussie au topic : " + topicName);
        System.out.println("En attente de messages...");

        // Boucle infinie d’écoute
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, key = %s, value = %s%n",
                        record.offset(), record.key(), record.value());
            }
        }
    }
}
