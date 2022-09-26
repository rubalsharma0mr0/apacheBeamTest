package aws;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class S3Example {
    public static void main(String[] args) {
        awsOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(awsOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(options.getAwsAccessKey(), options.getAwsSecretKey());

        options.setAwsCredentialsProvider(new AWSStaticCredentialsProvider(awsCredentials));

        PCollection<String> pCollectionInput = pipeline.apply(TextIO.read().from("s3://beam-udemy-training/input/user_order.csv"));

        pCollectionInput.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void processElement(ProcessContext processContext) {
                System.out.println(processContext.element());
            }
        }));
    }
}
