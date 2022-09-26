package others;

import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {

    String getInputFile();

    void setInputFile(String file);

    String getOutputFile();

    void setOutputFile(String file);

    String getExtension();

    void setExtension(String extension);

}
