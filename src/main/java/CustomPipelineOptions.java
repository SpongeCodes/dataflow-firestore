import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface CustomPipelineOptions extends PipelineOptions {

    // later this options can be implemented to add more customization to pipeline settings
    @Description("GCP Project id")
    String getProject();

    void setProject();

    @Description("the region to use for compute")
    @Default.String("asia-south1")
    String getRegion();
    void setRegion(String inputTopic);

    @Description("Input Pub/Sub topic")
    @Default.String("projects/your-gcp-project/topics/your-topic")
    String getInputTopic();
    void setInputTopic(String inputTopic);

    @Description("the window size to use in minutes")
    int getWindowTime();

    void setWindowTime(int windowTime);



}
