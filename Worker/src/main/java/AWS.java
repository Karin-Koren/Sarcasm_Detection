import java.io.IOException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.Paths;
import java.util.Base64;

public class AWS {
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;

    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    private static final AWS instance = new AWS();

    private static final String AMI_ID = "ami-0a3c3a20c09d6f377"; // Replace with your desired AMI ID

    private static final String TAG_KEY = "name";
    private static final String MANAGER_TAG_VALUE = "Manager";

    private AWS() {
        s3 = S3Client.builder().region(region1).build();
        sqs = SqsClient.builder().region(region1).build();
        ec2 = Ec2Client.builder().region(region2).build();
    }

    public static AWS getInstance() {
        return instance;
    }

    // Ec2
    public RunInstancesResponse createEc2(int min, int max, String kay, String value, String script) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceType(InstanceType.T2_MICRO)
                .imageId(AMI_ID)
                .maxCount(max)
                .minCount(min)
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(Tag.builder().key(kay).value(value).build())
                        .build())
                .userData(Base64.getEncoder().encodeToString(script/* THE SCRIPT */.getBytes()))

                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        System.out.println(value + "node started. id = " + instanceId);
        return response;
    }

    public void createManagerIfNotExists() {

        Filter tagFilter = Filter.builder()
                .name("tag:" + TAG_KEY)
                .values(MANAGER_TAG_VALUE)
                .build();

        DescribeInstancesRequest describeReq = DescribeInstancesRequest.builder()
                .filters(tagFilter)
                .build();

        DescribeInstancesResponse describeResponse = ec2.describeInstances(describeReq);

        if (!describeResponse.reservations().isEmpty())
            System.out.println("Manager is exist!");

        else {
            System.out.println("in else");
            createEc2(1, 1, TAG_KEY, MANAGER_TAG_VALUE, "");
        }

    }

    // S3
    public void createBucketIfNotExists(String bucketName) {
        try {
            s3.createBucket(CreateBucketRequest
                    .builder()
                    .bucket(bucketName)
                    .createBucketConfiguration(
                            CreateBucketConfiguration.builder()
                                    .locationConstraint(BucketLocationConstraint.US_WEST_2)
                                    .build())
                    .build());
            s3.waiter().waitUntilBucketExists(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());

        } catch (S3Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public void uploadFile(String bucketName, String localfileName, String key) {
        s3.putObject(PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build(),
                Paths.get(localfileName));
        System.out.println("File uploaded successfully!");
    }

    public void downloadFileFromS3(String bucketName, String key, Path localDownloadPath) {
        System.out.println("in Aws download...");
        try {
            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            ResponseInputStream<?> objectData = s3.getObject(getObjectRequest);
            System.out.println("in Aws before copy...");
            Files.copy(objectData, localDownloadPath, StandardCopyOption.REPLACE_EXISTING);
        } catch (S3Exception e) {
            System.err.println("Error retrieving object from S3: " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Error writing object to file: " + e.getMessage());
        }
    }

    public void deleteFileFromS3(String bucketName, String key) {
        System.out.println("aws - delete file");
        try {
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            s3.deleteObject(deleteObjectRequest);
            System.out.println("File deleted successfully from bucket: " + bucketName + ", Key: " + key);
        } catch (S3Exception e) {
            System.err.println("Error deleting file from S3: " + e.getMessage());
        }
    }

    // sqs
    public void createSqsQueue(String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        sqs.createQueue(createQueueRequest);
    }

    public void sendMessageToQueue(String queueUrl, String messageBody) {

        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build();

        sqs.sendMessage(sendMessageRequest);
    }

    public Message receiveMessage(String queueUrl) {

        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(1)
                .waitTimeSeconds(20)
                .build();

        ReceiveMessageResponse receiveMessageResponse = sqs.receiveMessage(receiveMessageRequest);

        if (!receiveMessageResponse.messages().isEmpty()) {
            return receiveMessageResponse.messages().get(0);
        }
        return null;
    }

    public void deleteMessage(String receiptHandle, String queueUrl) {
        DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                .queueUrl(queueUrl)
                .receiptHandle(receiptHandle)
                .build();
        sqs.deleteMessage(deleteMessageRequest);
    }

    public String getQueUrl(String queueName) {
        GetQueueUrlResponse getQueueUrlResponse = sqs
                .getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
        return getQueueUrlResponse.queueUrl();
    }

    public void close() {
        sqs.close();
    }

}