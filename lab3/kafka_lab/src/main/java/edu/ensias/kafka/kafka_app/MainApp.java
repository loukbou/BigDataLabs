package edu.ensias.kafka.kafka_app;

public class MainApp {

    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            System.out.println("Usage: java -jar kafka-multi-app-jar-with-dependencies.jar [producer|consumer]");
            return;
        }

        String mode = args[0].toLowerCase();

        switch (mode) {
            case "producer":
                WordProducer.main(new String[]{});
                break;

            case "consumer":
                WordCountConsumer.main(new String[]{});
                break;

            default:
                System.out.println("Argument inconnu : " + mode);
                System.out.println("Utilisez : producer ou consumer");
        }
    }
}
