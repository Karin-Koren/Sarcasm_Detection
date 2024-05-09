import java.io.IOException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class AWS {
    private final S3Client s3;
    private final SqsClient sqs;
    private final Ec2Client ec2;

    public static Region region1 = Region.US_WEST_2;
    public static Region region2 = Region.US_EAST_1;

    private static final AWS instance = new AWS();

    private static final String AMI_ID = "ami-0a3c3a20c09d6f377";

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

    public RunInstancesResponse createEc2(int min, int max, String value, String script) {
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .instanceInitiatedShutdownBehavior(ShutdownBehavior.TERMINATE)
                .instanceType(InstanceType.M4_LARGE)
                .imageId(AMI_ID)
                .maxCount(max)
                .minCount(min)
                .tagSpecifications(TagSpecification.builder()
                        .resourceType(ResourceType.INSTANCE)
                        .tags(Tag.builder().key(TAG_KEY).value(value).build())
                        .build())
                .userData(Base64.getEncoder().encodeToString(script/*
                                                                    * THE SCRIPT
                                                                    */.getBytes()))
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
                .build();

        RunInstancesResponse response = ec2.runInstances(runRequest);
        String instanceId = response.instances().get(0).instanceId();

        System.out.println(value + "node started. id = " + instanceId);
        return response;
    }

    // public String createEC2(String script, String tagName, int numberOfInstances)
    // {
    // RunInstancesRequest runRequest = (RunInstancesRequest)
    // RunInstancesRequest.builder()
    // .instanceType(InstanceType.T2_MICRO)
    // .imageId(AMI_ID)
    // .maxCount(numberOfInstances)
    // .minCount(1)
    // .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("LabInstanceProfile").build())
    // .userData(Base64.getEncoder().encodeToString((script).getBytes()))
    // .build();

    // RunInstancesResponse response = ec2.runInstances(runRequest);

    // String instanceId = response.instances().get(0).instanceId();

    // software.amazon.awssdk.services.ec2.model.Tag tag = Tag.builder()
    // .key("Name")
    // .value(tagName)
    // .build();

    // CreateTagsRequest tagRequest = (CreateTagsRequest)
    // CreateTagsRequest.builder()
    // .resources(instanceId)
    // .tags(tag)
    // .build();

    // try {
    // ec2.createTags(tagRequest);
    // System.out.printf(
    // "[DEBUG] Successfully started EC2 instance %s based on AMI %s\n",
    // instanceId, AMI_ID);

    // } catch (Ec2Exception e) {
    // System.err.println("[ERROR] " + e.getMessage());
    // System.exit(1);
    // }
    // return instanceId;
    // }

    public void createManagerIfNotExists() {

        if (getNumOfInstancesCount("Manager") != 0)
            System.out.println("Manager is exist!");

        else {
            System.out.println("in else");
            createEc2(1, 1, MANAGER_TAG_VALUE, "");
        }
    }

    public int getNumOfInstancesCount(String tag) {
        int count = 0;
        try {
            // Create a filter for instances with a tag key "worker"
            Filter filter = Filter.builder()
                    .name("tag:" + TAG_KEY)
                    .values(tag)
                    .build();

            // Create a request to describe instances with the specified filter
            DescribeInstancesRequest request = DescribeInstancesRequest.builder()
                    .filters(filter)
                    .build();

            // Call the EC2 service to get instances
            DescribeInstancesResponse response = ec2.describeInstances(request);

            // Count the instances that are not terminated
            for (Reservation reservation : response.reservations()) {
                for (Instance instance : reservation.instances()) {
                    if (!instance.state().name().toString().equals("terminated")
                            && !instance.state().name().toString().equals("stopped")) {
                        count++;
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Error retrieving EC2 instances: " + e.getMessage());
        }

        return count;
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

    public List<String> listS3Objects(String bucketName, String prefix) {
        List<String> fileNames = new LinkedList<String>();
        // Create a request to list objects
        ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                .bucket(bucketName)
                .prefix(prefix)
                .delimiter("/") // Use delimiter to simulate directory structure
                .build();

        // List objects and print their names
        ListObjectsV2Response listRes = s3.listObjectsV2(listReq);
        for (S3Object s3Object : listRes.contents()) {
            fileNames.add(s3Object.key());
        }
        return fileNames;
    }

    public boolean doesObjectExist(String bucketName, String key) {
        try {
            HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .build();

            HeadObjectResponse headObjectResponse = s3.headObject(headObjectRequest);
            return true; // If the object exists
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false; // If the object does not exist
            }
            // Handle other exceptions
            System.err.println("Error checking object existence: " + e.getMessage());
            return false;
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

    public void deleteBucket(String bucketName) {
        try {
            // First, empty the bucket
            ListObjectsV2Response listObjectsV2Response = s3
                    .listObjectsV2(ListObjectsV2Request.builder().bucket(bucketName).build());
            for (S3Object s3Object : listObjectsV2Response.contents()) {
                s3.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(s3Object.key()).build());
            }

            // Then delete the bucket
            s3.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build());
            System.out.println("Bucket deleted successfully: " + bucketName);
        } catch (S3Exception e) {
            System.err.println("Error deleting S3 bucket: " + e.getMessage());
        }
    }

    // sqs
    public void createSqsQueue(String queueName) {
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();
        sqs.createQueue(createQueueRequest);
    }

    public void createQueueIfNotExists(String queueName) {
        try {
            String queueUrl = null;
            // Try to get the URL of the existing queue
            try {
                GetQueueUrlResponse getQueueUrlResponse = sqs
                        .getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
                queueUrl = getQueueUrlResponse.queueUrl();
                System.out.println("Queue already exists: " + queueUrl);
            } catch (QueueDoesNotExistException e) {
                System.out.println("Queue does not exist. Creating a new queue: " + queueName);
            }

            // If the queue doesn't exist, create it
            if (queueUrl == null) {
                CreateQueueResponse createQueueResponse = sqs
                        .createQueue(CreateQueueRequest.builder().queueName(queueName).build());
                System.out.println("Created new queue: " + createQueueResponse.queueUrl());
            }
        } catch (SqsException e) {
            System.err.println("Error interacting with SQS: " + e.awsErrorDetails().errorMessage());
        }
    }

    public String createSqsQueueWithVisibilityTimeout(String queueName, int visibilityTimeoutInSeconds) {

        // Set visibility timeout (in seconds)
        Map<QueueAttributeName, String> attributes = new HashMap<>();
        attributes.put(QueueAttributeName.VISIBILITY_TIMEOUT, String.valueOf(visibilityTimeoutInSeconds));

        // Create a CreateQueueRequest
        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .attributes(attributes)
                .build();

        // Create the SQS queue
        CreateQueueResponse createQueueResponse = sqs.createQueue(createQueueRequest);

        // Return the queue URL
        return createQueueResponse.queueUrl();
    }

    public void sendMessageToQueue(String queueUrl, String messageBody) {
        System.out.println(".....in sendmag :" + messageBody);
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

    // Close the SQS client when no longer needed

    public void setupBasicQueuePolicy(String queueUrl) {
        String basicPolicy = "{"
                + "\"Version\": \"2012-10-17\","
                + "\"Statement\": ["
                + "  {"
                + "    \"Effect\": \"Allow\","
                + "    \"Principal\": \"*\","
                + "    \"Action\": ["
                + "       \"sqs:SendMessage\","
                + "       \"sqs:ReceiveMessage\","
                + "       \"sqs:DeleteMessage\","
                + "       \"sqs:GetQueueAttributes\""
                + "    ],"
                + "    \"Resource\": \"" + queueUrl + "\""
                + "  }"
                + "]"
                + "}";

        Map<QueueAttributeName, String> attributes = new HashMap<>();
        attributes.put(QueueAttributeName.POLICY, basicPolicy);

        sqs.setQueueAttributes(SetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributes(attributes)
                .build());
    }

    public void closeQueue(String queueUrl) {
        String denySendMessagePolicy = "{"
                + "\"Version\": \"2012-10-17\","
                + "\"Statement\": ["
                + "  {"
                + "    \"Effect\": \"Deny\","
                + "    \"Principal\": \"*\","
                + "    \"Action\": \"sqs:SendMessage\","
                + "    \"Resource\": \"" + queueUrl + "\""
                + "  }"
                + "]"
                + "}";

        Map<QueueAttributeName, String> attributes = new HashMap<>();
        attributes.put(QueueAttributeName.POLICY, denySendMessagePolicy);

        SetQueueAttributesRequest setAttrsRequest = SetQueueAttributesRequest.builder()
                .queueUrl(queueUrl)
                .attributes(attributes)
                .build();

        sqs.setQueueAttributes(setAttrsRequest);

        System.out.println("Queue is now closed for new messages.");
    }

    public String getQueUrl(String queueName) {
        GetQueueUrlResponse getQueueUrlResponse = sqs
                .getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
        return getQueueUrlResponse.queueUrl();
    }

    public void close() {
        sqs.close();
    }

    public void deleteQueue(String queueUrl) {
        try {
            sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
            System.out.println("Queue deleted successfully: " + queueUrl);
        } catch (SqsException e) {
            System.err.println("Error deleting SQS queue: " + e.getMessage());
        }
    }

}