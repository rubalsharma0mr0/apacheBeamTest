package transformation;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

import java.util.Objects;

/*transformation.GroupByKey internal Work:
1. Read from file
2. Prepare key value pair
3. Prepare key value pair again just value is iterable
4. submission or any other aggregation operation
*/

class StringToKV extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public void processElement(ProcessContext processContext) {
        String input = processContext.element();
        String[] arr = Objects.requireNonNull(input).split(",");
        processContext.output(KV.of(arr[0], Integer.valueOf(arr[3])));
    }
}

class KVToString extends DoFn<KV<String, Iterable<Integer>>, String> {
    @ProcessElement
    public void processElement(ProcessContext processContext) {
        String key = Objects.requireNonNull(processContext.element()).getKey();
        Iterable<Integer> value = Objects.requireNonNull(processContext.element()).getValue();

        int sum = 0;
        for (Integer integer : Objects.requireNonNull(value)) {
            sum = sum + integer;
        }
        processContext.output(key + "," + sum);
    }

}

public class GroupByKey {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollectionRead = pipeline.apply(TextIO.read()
                .from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/GroupByKey_data.csv"));

        PCollection<KV<String, Integer>> kvpCollection = pCollectionRead.apply(ParDo.of(new StringToKV()));

        PCollection<KV<String, Iterable<Integer>>> pCollectionSum = kvpCollection.apply(org.apache.beam.sdk.transforms.GroupByKey.create());

        PCollection<String> pCollectionWrite = pCollectionSum.apply(ParDo.of(new KVToString()));
        pCollectionWrite.apply(TextIO.write().to("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/transformation.GroupByKey.csv")
                .withHeader("ID, SUM").withNumShards(1).withSuffix(".csv"));
        pipeline.run();
    }
}
