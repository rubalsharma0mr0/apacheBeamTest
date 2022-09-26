package mapElement;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MapElement_TypeDescriptors {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollectionRead = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/customer.csv"));

        //MapElement Using TypeDescriptors
        PCollection<String> pCollectionWrite = pCollectionRead.apply(MapElements.into(TypeDescriptors.strings()).via((String obj) -> obj.toUpperCase()));
        pCollectionWrite.apply(TextIO.write().to("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/mapElement.MapElement_TypeDescriptors.csv").withNumShards(1).withSuffix(".csv"));
        pipeline.run();

    }
}

