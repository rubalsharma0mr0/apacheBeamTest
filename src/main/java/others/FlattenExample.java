package others;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class FlattenExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollectionRead1 = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/customer_1.csv"));
        PCollection<String> pCollectionRead2 = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/customer_2.csv"));
        PCollection<String> pCollectionRead3 = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/customer_3.csv"));

        //Using Flatten to merge multiple PCollection into 1
        PCollectionList<String> stringPCollectionList = PCollectionList.of(pCollectionRead1).and(pCollectionRead2).and(pCollectionRead3);
        PCollection<String> pCollectionWrite = stringPCollectionList.apply(Flatten.pCollections());
        pCollectionWrite.apply(TextIO.write().to("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/others.FlattenExample.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));

        pipeline.run();

    }
}
