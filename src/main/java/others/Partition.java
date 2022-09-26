package others;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

class MyPartition implements org.apache.beam.sdk.transforms.Partition.PartitionFn<String> {

    @Override
    public int partitionFor(String elem, int numPartitions) {
        String[] arr = elem.split(",");

        if (arr[3].contains("Los Angeles")) {
            return 0;
        } else if (arr[3].contains("Phoenix")) {
            return 1;
        } else {
            return 2;
        }
    }
}

public class Partition {
    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollectionRead = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/others.Partition.csv"));
        PCollectionList<String> partition = pCollectionRead.apply(org.apache.beam.sdk.transforms.Partition.of(3, new MyPartition()));

        PCollection<String> pCollection0 = partition.get(0);
        PCollection<String> pCollection1 = partition.get(1);
        PCollection<String> pCollection2 = partition.get(2);

        pCollection0.apply(TextIO.write().to("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/others.Partition/pCollection0.csv")
                .withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
        pCollection1.apply(TextIO.write().to("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/others.Partition/pCollection1.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
        pCollection2.apply(TextIO.write().to("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/others.Partition/pCollection2.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));

        pipeline.run();
    }
}
