package others;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;


class Customer extends DoFn<String, String> {

    /*
    DoFn<InputT, OutputT>:

    executed with ParDo;
    exposed to the context (timestamp, window pane, etc);
    can consume side inputs;
    can produce multiple outputs or no outputs at all;
    can produce side outputs;
    can use Beam's persistent state APIs;
    dynamically typed;
    example use case: read objects from a stream, filter, accumulate them, perform aggregations, convert them, and dispatch to different outputs;
    */

    @ProcessElement
    public void processElement(ProcessContext processContext) {
        String line = processContext.element(); // reading CSV line by line
        assert line != null;
        String[] arr = line.split(",");

        if (arr[3].equals("Los Angeles") && arr[1].equals("Art")) {
            processContext.output(line);
        }
    }

}

public class ParDoExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollectionRead = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/customer_pardo.csv"));

        //PadDo
        PCollection<String> pCollectionWrite = pCollectionRead.apply(ParDo.of(new Customer()));
        pCollectionWrite.apply(TextIO.write().to("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/others.ParDoExample.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
        pipeline.run();
    }
}
