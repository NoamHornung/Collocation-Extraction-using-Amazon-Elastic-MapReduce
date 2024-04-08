import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClient;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;


public class Main {

    public static void main(String[] args) {

        String minPmi = args[0];
        String relMinPmi = args[1];
        String language = args[2];

        int numOfInstances = 9;

        String bucketName = "ass2bucket1212"; // TODO change to your bucket name

        String HebrewInputPath ="s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data";
        String EnglishInputPath = "s3://datasets.elasticmapreduce/ngrams/books/20090715/eng-all/2gram/data";

        String inputPath;
        String outputsFolder;

        if(language.equals("hebrew")){
            inputPath = HebrewInputPath;
            outputsFolder = "/heb_outputs";
        }
        else{
            inputPath = EnglishInputPath;
            outputsFolder = "/eng_outputs";
        }

        String step1JarPath = "s3://"+bucketName+"/jars/step1-jar-with-dependencies.jar"; 
        String step2JarPath = "s3://"+bucketName+"/jars/step2-jar-with-dependencies.jar"; 
        String step3JarPath = "s3://"+bucketName+"/jars/step3-jar-with-dependencies.jar";
        String step4JarPath = "s3://"+bucketName+"/jars/step4-jar-with-dependencies.jar";;
        String step5JarPath = "s3://"+bucketName+"/jars/step5-jar-with-dependencies.jar";;
        String step6JarPath = "s3://"+bucketName+"/jars/step6-jar-with-dependencies.jar";
        String step1OutputPath = "s3://"+bucketName+outputsFolder+"/step1-output";
        String step2OutputPath = "s3://"+bucketName+outputsFolder+"/step2-output";
        String step3OutputPath = "s3://"+bucketName+outputsFolder+"/step3-output";
        String step4OutputPath = "s3://"+bucketName+outputsFolder+"/step4-output";
        String step5OutputPath = "s3://"+bucketName+outputsFolder+"/step5-output";
        String logURI = "s3://"+bucketName+"/logs";
        
        
        AWSCredentials credentials = new ProfileCredentialsProvider("default").getCredentials();
        AmazonElasticMapReduce mapReduce = AmazonElasticMapReduceClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .withRegion(Regions.US_EAST_1)
            .build();

        // step1
        HadoopJarStepConfig hadoopJarStep1 = new HadoopJarStepConfig()
            .withJar(step1JarPath)
            .withArgs(inputPath, step1OutputPath, language);

        StepConfig step1Config = new StepConfig()
            .withName("step1")
            .withHadoopJarStep(hadoopJarStep1)
            .withActionOnFailure("TERMINATE_JOB_FLOW");

        // step2
        HadoopJarStepConfig hadoopJarStep2 = new HadoopJarStepConfig()
            .withJar(step2JarPath)
            .withArgs(step1OutputPath, step2OutputPath);

        StepConfig step2Config = new StepConfig()
            .withName("step2")
            .withHadoopJarStep(hadoopJarStep2)
            .withActionOnFailure("TERMINATE_JOB_FLOW");

        // step3
        HadoopJarStepConfig hadoopJarStep3 = new HadoopJarStepConfig()
            .withJar(step3JarPath)
            .withArgs(step2OutputPath, step3OutputPath);

        StepConfig step3Config = new StepConfig()
            .withName("step3")
            .withHadoopJarStep(hadoopJarStep3)
            .withActionOnFailure("TERMINATE_JOB_FLOW");

        // step4
        HadoopJarStepConfig hadoopJarStep4 = new HadoopJarStepConfig()
            .withJar(step4JarPath)
            .withArgs(step3OutputPath, step4OutputPath);

        StepConfig step4Config = new StepConfig()
            .withName("step4")
            .withHadoopJarStep(hadoopJarStep4)
            .withActionOnFailure("TERMINATE_JOB_FLOW");

        // step5
        HadoopJarStepConfig hadoopJarStep5 = new HadoopJarStepConfig()
            .withJar(step5JarPath)
            .withArgs(step4OutputPath, step5OutputPath, minPmi, relMinPmi);

        StepConfig step5Config = new StepConfig()
            .withName("step5")
            .withHadoopJarStep(hadoopJarStep5)
            .withActionOnFailure("TERMINATE_JOB_FLOW");

        StepConfig[] steps = {step1Config, step2Config, step3Config, step4Config, step5Config};

        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
            .withInstanceCount(numOfInstances)
            .withMasterInstanceType(InstanceType.M4Large.toString())
            .withSlaveInstanceType(InstanceType.M4Large.toString())
            .withHadoopVersion("2.9.2").withEc2KeyName("vockey")
            .withKeepJobFlowAliveWhenNoSteps(false)
            .withPlacement(new PlacementType("us-east-1a"));
        
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
            .withName("collocation extraction")
            .withInstances(instances)
            .withSteps(steps)
            .withLogUri(logURI)
            .withServiceRole("EMR_DefaultRole")
            .withJobFlowRole("EMR_EC2_DefaultRole")
            .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = mapReduce.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("Ran job flow with id: " + jobFlowId);
    }


}