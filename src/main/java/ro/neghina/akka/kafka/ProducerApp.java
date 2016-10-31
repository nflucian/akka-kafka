package ro.neghina.akka.kafka;

import akka.actor.ActorSystem;
import akka.kafka.ProducerSettings;
import akka.kafka.javadsl.Producer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Source;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerApp {

    public static void main(String[] args) {
        final ActorSystem actorSystem = ActorSystem.create("akka-producer");
        final Materializer materializer = ActorMaterializer.create(actorSystem);

        final ProducerSettings<byte[], String> producerSettings = ProducerSettings
                .create(actorSystem, new ByteArraySerializer(), new StringSerializer())
                .withBootstrapServers("localhost:9092");

        Source.range(10000, 20000)
                .map(n -> n.toString())
                .map(elem -> {
                    System.out.println(elem);
                    return elem;
                })
                .map(elem -> new ProducerRecord<byte[], String>("test", elem))
                .runWith(Producer.plainSink(producerSettings), materializer);
    }
}
