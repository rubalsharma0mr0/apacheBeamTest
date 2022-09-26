package others;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Map;
import java.util.Objects;

public class SideInputs {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        PCollection<KV<String, String>> pCollectionReturn = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/return.csv")).apply(ParDo.of(new DoFn<String, KV<String, String>>() {

            @ProcessElement
            public void process(ProcessContext processContext) {
                String[] arr = Objects.requireNonNull(processContext.element()).split(",");
                processContext.output(KV.of(arr[0], arr[1]));
            }
        }));

        PCollectionView<Map<String, String>> pMap = pCollectionReturn.apply(View.asMap());

        PCollection<String> pCollectionRead = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/cust_order.csv"));
        PCollection<Void> pCollectionWrite = pCollectionRead.apply(ParDo.of(new DoFn<String, Void>() {

            /*Inside your DoFn subclass, you’ll write a method annotated with @ProcessElement where you provide the actual processing logic.
            You don’t need to manually extract the elements from the input collection; the Beam SDKs handle that for you.
            Your @ProcessElement method should accept a parameter tagged with @Element, which will be populated with the input element.
            In order to output elements, the method can also take a parameter of type OutputReceiver which provides a method for emitting elements.
            The parameter types must match the input and output types of your DoFn or the framework will raise an error.
            Note: @Element and OutputReceiver were introduced in Beam 2.5.0; if using an earlier release of Beam,
            a ProcessContext parameter should be used instead.*/

            @ProcessElement
            public void process(ProcessContext processContext) {
                Map<String, String> inputView = processContext.sideInput(pMap);
                String[] arr = Objects.requireNonNull(processContext.element()).split(",");
                String customerName = inputView.get(arr[0]);
//                System.out.println(customerName);
                if (customerName == null) {
                    System.out.println(processContext.element());
                }
            }
        }).withSideInputs(pMap));

//        pCollectionWrite.apply(TextIO.write().to("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/others.ParDoExample.csv").withHeader("Id,Name,Last Name,City").withNumShards(1).withSuffix(".csv"));
        pipeline.run();

    }
}
