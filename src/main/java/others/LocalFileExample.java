package others;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;

public class LocalFileExample {
    public static void main(String[] args) {

        MyOptions myOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline pipeline = Pipeline.create();

        //Reading File using PTransformation
        PCollection<String> output = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/input.csv"));
        //        PCollection<String> output = pipeline.apply(TextIO.read().from(myOptions.getInputFile()));

        output.apply(TextIO.write().to("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/output.csv").withNumShards(1).withSuffix(".csv"));
        //output.apply(TextIO.write().to(myOptions.getOutputFile()).withNumShards(1).withSuffix(myOptions.getExtension()));
        pipeline.run();

    }
}
/*java -cp apacheBeam-1.0-SNAPSHOT-jar-with-dependencies.jar apacheBeam.others.LocalFileExample
 --inputFile="/home/rubal/IdeaProjects/apacheBeam/Lib/Input/input.csv"
 --outputFile="/home/rubal/IdeaProjects/apacheBeam/Lib/Output/output.csv"
 --extension=".csv"*/
