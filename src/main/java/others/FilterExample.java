package others;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;


class MyFilter implements SerializableFunction<String, Boolean> {

    @Override
    public Boolean apply(String input) {

        return input.contains("Percentage");
    }
}

public class FilterExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> pCollectionRead = pipeline.apply(TextIO.read().from("/home/rubal/IdeaProjects/apacheBeam/Lib/Input/annual-enterprise-survey-2020-financial-year-provisional-csv.csv"));

        //Filter
        PCollection<String> pCollectionWrite = pCollectionRead.apply(Filter.by(new MyFilter()));
        pCollectionWrite.apply(TextIO.write().to("/home/rubal/IdeaProjects/apacheBeam/Lib/Output/others.FilterExample.csv").withHeader("Year,Industry_aggregation_NZSIOC,Industry_code_NZSIOC,Industry_name_NZSIOC,Units,Variable_code,Variable_name,Variable_category,Value,Industry_code_ANZSIC06").withNumShards(1).withSuffix(".csv"));
        pipeline.run();
    }
}
