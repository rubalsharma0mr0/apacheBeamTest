package transformation;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.values.PCollection;

//can be used to remove duplicate records
public class DistinctExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollectionRead = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/Distinct.csv"));
        PCollection<String> pCollectionWrite = pCollectionRead.apply(Distinct.create());

        pCollectionWrite.apply(TextIO.write().to("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/transformation.DistinctExample.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
        pipeline.run();
    }
}
