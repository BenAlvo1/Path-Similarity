import software.amazon.awssdk.awscore.AwsRequestOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.emr.EmrClient;
import software.amazon.awssdk.services.emr.model.*;

public class PathsSimilarity {
    public static final String STEP1_JAR_PATH = "s3://path-similarity-jars/Step1.jar";
    public static final String ROUND3_JAR_PATH = "s3://path-similarity-jars/Round3.jar";


    public static void main(String[] args) {

        EmrClient emr = EmrClient.builder()
                .region(Region.US_EAST_1)
                .build();

        HadoopJarStepConfig step1 = HadoopJarStepConfig.builder()
                .jar(STEP1_JAR_PATH).build();

        StepConfig step1Config = StepConfig.builder()
                .name("Step1")
                .hadoopJarStep(step1)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();

        HadoopJarStepConfig step3 = HadoopJarStepConfig.builder()
                .jar(ROUND3_JAR_PATH)
                .build();

        StepConfig step3Config = StepConfig.builder()
                .name("Round3")
                .hadoopJarStep(step3)
                .actionOnFailure("TERMINATE_JOB_FLOW")
                .build();


        JobFlowInstancesConfig instancesConfig = JobFlowInstancesConfig.builder()
                .instanceCount(8)
                .masterInstanceType(InstanceType.M4_XLARGE.toString())
                .slaveInstanceType(InstanceType.M4_XLARGE.toString())
                .hadoopVersion("3.3.2")
                .ec2KeyName("vockey")
                .placement(PlacementType.builder().build())
                .keepJobFlowAliveWhenNoSteps(false)
                .build();

        RunJobFlowRequest request = RunJobFlowRequest.builder()
                .name("path-similarity")
                .instances(instancesConfig)
                .steps(step1Config, step3Config)
                .logUri("s3://path-similarity-logs/")
                .serviceRole("EMR_DefaultRole")
                .jobFlowRole("EMR_EC2_DefaultRole")
                .releaseLabel("emr-5.11.0")
                .build();
        RunJobFlowResponse response = emr.runJobFlow(request);
        System.out.println("new cluster created: " + response.jobFlowId());
    }
}

