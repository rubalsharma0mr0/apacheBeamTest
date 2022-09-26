package Parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;


class PrintElem extends SimpleFunction<GenericRecord, Void> {

    @Override
    public Void apply(GenericRecord input) {

        System.out.println(input.get("SessionId"));
        System.out.println("SessionId : " + input.get("SessionId"));
        System.out.println("UserId" + input.get("UserId"));
        System.out.println("UserName" + input.get("UserName"));
        System.out.println("VideoId" + input.get("VideoId"));
        System.out.println("Duration" + input.get("Duration"));
        System.out.println("StartedTime" + input.get("StartedTime"));
        System.out.println("Sex" + input.get("Sex"));

        return null;
    }
}


public class ParquetIOWriteExample {

    public static void main(String[] args) {

        Pipeline p = Pipeline.create();
        Schema schema = ParquetSchema.getSchema();

        PCollection<GenericRecord> pOutput =
                p.apply(ParquetIO.read(schema).from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/Parquet/user.csv"));

        pOutput.apply(MapElements.via(new PrintElem()));

        p.run();
    }
}