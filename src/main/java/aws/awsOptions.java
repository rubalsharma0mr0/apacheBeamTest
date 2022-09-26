package aws;

import org.apache.beam.sdk.io.aws.options.S3Options;
import org.apache.beam.sdk.options.PipelineOptions;

public interface awsOptions extends PipelineOptions, S3Options {

    void setAWSAccessKey(String val);

    String getAwsAccessKey();

    void setAWSSecretKey(String val);

    String getAwsSecretKey();

    String getAWSRegion();

    void setAWSRegion(String val);

}
