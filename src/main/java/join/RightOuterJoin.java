package join;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import java.util.Objects;


class RightOrderParsing extends DoFn<String, KV<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext processContext) {
        String[] split = Objects.requireNonNull(processContext.element()).split(",");
        String key = split[0];
        String value = split[1] + "," + split[2] + "," + split[3];
//        String value = split[3];
        processContext.output(KV.of(key, value));
    }

}

class RightUserParsing extends DoFn<String, KV<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext processContext) {
        String[] split = Objects.requireNonNull(processContext.element()).split(",");
        String key = split[0];
        String value = split[1];
        processContext.output(KV.of(key, value));
    }

}

public class RightOuterJoin {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String, String>> pCollectionOrder = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/join/user_order.csv")).apply(ParDo.of(new RightOrderParsing()));
        PCollection<KV<String, String>> pCollectionUser = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/join/p_user.csv")).apply(ParDo.of(new RightUserParsing()));

        //Tuple Tag
        final TupleTag<String> orderTupleTag = new TupleTag<>();
        final TupleTag<String> userTupleTag = new TupleTag<>();

        //combining tuple
        PCollection<KV<String, CoGbkResult>> tupleCollection = KeyedPCollectionTuple.of(orderTupleTag, pCollectionOrder).and(userTupleTag, pCollectionUser).apply(CoGroupByKey.create());

        PCollection<String> applyOutput = tupleCollection.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                String key = Objects.requireNonNull(processContext.element()).getKey();
                CoGbkResult value = Objects.requireNonNull(processContext.element()).getValue();

                assert value != null;
                Iterable<String> allOrder = value.getAll(orderTupleTag);
                Iterable<String> allUser = value.getAll(userTupleTag);

                for (String user : allUser) {
                    if (allOrder.iterator().hasNext()) {
                        for (String order : allOrder) {
                            processContext.output(key + "," + user + "," + order);
                        }
                    } else {
                        processContext.output(key + "," + user + "," + "null, null, null");
                    }
                }
            }
        }));

        applyOutput.apply(TextIO.write().to("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/join.RightOuterJoin.csv").withHeader("ID,User Name, ProductID, OrderID, Amount ").withNumShards(1).withSuffix(".csv"));

        pipeline.run();
    }
}