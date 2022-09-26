import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaReadTest {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<KafkaRecord<String, String>> pCollectionRead = pipeline.apply(KafkaIO.<String, String>read().withBootstrapServers("13.233.192.202:31234").withTopic("kafka-cp-kafka-connect-config").withKeyDeserializer(StringDeserializer.class).withValueDeserializer(StringDeserializer.class));


        PCollection<KafkaRecord<String, String>> output = pCollectionRead.apply(ParDo.of(new DoFn<KafkaRecord<String, String>, KafkaRecord<String, String>>() {
            @ProcessElement
            public void process(ProcessContext c) {
                KafkaRecord<String, String> data = c.element();
                c.output(data);
            }
        }));
        pipeline.run();
    }
}
