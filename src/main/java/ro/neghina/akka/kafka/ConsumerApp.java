package ro.neghina.akka.kafka;

import akka.Done;
import akka.actor.ActorSystem;
import akka.kafka.ConsumerSettings;
import akka.kafka.Subscriptions;
import akka.kafka.javadsl.Consumer;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import akka.stream.javadsl.Sink;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class ConsumerApp {

    public static void main(String[] args) {
        final ActorSystem actorSystem = ActorSystem.create("akka-consumer");
        final Materializer materializer = ActorMaterializer.create(actorSystem);

        final ConsumerSettings<byte[], String> consumerSettings = ConsumerSettings.create(
                actorSystem, new ByteArrayDeserializer(), new StringDeserializer())
                .withBootstrapServers("localhost:9092")
                .withGroupId("myGroup")
                .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Rocket rocket = new Rocket();

        Consumer.committableSource(consumerSettings, Subscriptions.topics("test"))
                .mapAsync(1, msg -> rocket.launch(msg.record().value()).thenApply(done -> msg))
                .mapAsync(1, msg -> msg.committableOffset().commitJavadsl())
                .runWith(Sink.ignore(), materializer);
    }

    static class Rocket {
        public CompletionStage<Done> launch(String destination) {
            System.out.println("Rocket launched to " + destination);
            return CompletableFuture.completedFuture(Done.getInstance());
        }
    }
}
