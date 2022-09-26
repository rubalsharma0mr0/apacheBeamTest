package mapElement;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

class User extends SimpleFunction<String, String> {

    /*
    SimpleFunction<InputT, OutputT>:

    simple input to output mapping function;
    single input produces single output;
    statically typed, you have to @Override the apply() method;
    doesn't depend on computation context;
    can't use Beam state APIs;
    example use case: MapElements.via(simpleFunction) to convert/modify elements one by one, producing one output for each element;
    */

    @Override
    public String apply(String input) {

        String[] arr = input.split(",");
        String SessionId = arr[0];
        String UserId = arr[1];
        String UserName = arr[2];
        String VideoId = arr[3];
        String Duration = arr[4];
        String StartTime = arr[5];
        String Sex = arr[6];

        String updatedOutput;

        if (Sex.equals("1")) {
            updatedOutput = SessionId + "," + UserId + "," + UserName + "," + VideoId + "," + Duration + "," + StartTime + "," + "M";
        } else if (Sex.equals("2")) {
            updatedOutput = SessionId + "," + UserId + "," + UserName + "," + VideoId + "," + Duration + "," + StartTime + "," + "F";
        } else {
            updatedOutput = SessionId + "," + UserId + "," + UserName + "," + VideoId + "," + Duration + "," + StartTime + "," + Sex;
        }
        return updatedOutput;
    }
}

public class MapElement_SimpleFunction {
    public static void main(String[] args) {

        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollectionRead = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/user.csv"));
        PCollection<String> pCollectionWrite = pCollectionRead.apply(MapElements.via(new User()));
        pCollectionWrite.apply(TextIO.write().to("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/mapElement.MapElement_SimpleFunction.csv").withNumShards(1).withSuffix(".csv"));
        pipeline.run();
    }
}
