package Parquet;
/*
Apache Parquet is an open source, column-oriented data file format designed for efficient data storage and retrieval.
It provides efficient data compression and encoding schemes with enhanced performance to handle complex data in bulk.
Apache Parquet is designed to be a common interchange format for both batch and interactive workloads.*/

/*
    Characteristics of Parquet:
        1.Free and open source file format.
        2. Language agnostic.
        3. Column-based format - files are organized by column, rather than by row, which saves storage space and speeds up analytics queries.
        4. Used for analytics (OLAP) use cases, typically in conjunction with traditional OLTP databases.
        5. Highly efficient data compression and decompression.
        6. Supports complex data types and advanced nested data structures.

    Benefits of Parquet:
        1. Good for storing big data of any kind (structured data tables, images, videos, documents).

        2. Saves on cloud storage space by using highly efficient column-wise compression,
        and flexible encoding schemes for columns with different data types.

        3. Increased data throughput and performance using techniques like data skipping, whereby queries that fetch specific column values
        need not read the entire row of data.
*/


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

class ParquetSchema {
    public static Schema getSchema() {
        String SCHEMA_STRING = "{\"type\": \"record\",\n" + " \"name\": \"ParquetExample\",\n" + " \"fields\": [\n" + "     {\"name\": \"SessionId\", \"type\": \"string\"},\n" + "     {\"name\": \"UserId\", \"type\": \"string\"},\n" + "     {\"name\": \"UserName\", \"type\": \"string\"},\n" + "     {\"name\": \"VideoId\", \"type\": \"string\"},\n" + "     {\"name\": \"Duration\", \"type\": \"int\"},\n" + "     {\"name\": \"StartedTime\", \"type\": \"string\"},\n" + "     {\"name\": \"Sex\", \"type\": \"string\"}\n" + " ]\n" + "}";
        return new Schema.Parser().parse(SCHEMA_STRING);
    }
}

class ConvertCSVTOGeneric extends SimpleFunction<String, GenericRecord> {
    @Override
    public GenericRecord apply(String input) {

        String[] arr = input.split(",");
        Schema schema = ParquetSchema.getSchema();

        GenericRecord record = new GenericData.Record(schema);
        record.put("SessionId", arr[0]);
        record.put("UserId", arr[1]);
        record.put("UserName", arr[2]);
        record.put("VideoId", arr[3]);
        record.put("Duration", Integer.parseInt(arr[4]));
        record.put("StartedTime", arr[5]);
        record.put("Sex", arr[6]);

        return record;
    }
}

public class ParquetIOExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();

        Schema schema = ParquetSchema.getSchema();

        PCollection<GenericRecord> genericRecordPCollection = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/Parquet/user.csv")).apply(MapElements.via(new ConvertCSVTOGeneric())).setCoder(AvroCoder.of(GenericRecord.class, schema));

        genericRecordPCollection.apply(FileIO.<GenericRecord>write().via(ParquetIO.sink(schema)).to("/home/rubal/IdeaProjects/apacheBeam").withNumShards(1).withSuffix(".parquet"));

        pipeline.run();
    }
}
