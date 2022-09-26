package transformation;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

//to count total number of records in any PCollection
public class CountExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollectionRead1 = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/annual-enterprise-survey-2020-financial-year-provisional-csv.csv"));
        PCollection<String> pCollectionRead2 = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/Count.csv"));

        PCollection<Long> pCollectionCount = pCollectionRead1.apply(Count.globally());
        PCollection<Long> pCollectionCount1 = pCollectionRead2.apply(Count.globally());

        pCollectionCount.apply(ParDo.of(new DoFn<Long, Void>() {

            @ProcessElement
            public void processElement(ProcessContext processContext) {
                System.out.println("PCollection 0 has: " + processContext.element() + " records. ");
            }
        }));

        pCollectionCount1.apply(ParDo.of(new DoFn<Long, Void>() {

            @ProcessElement
            public void processElement(ProcessContext processContext) {
                System.out.println("PCollection 1 has: " + processContext.element() + " records. ");
            }
        }));
        pipeline.run();
    }
}
