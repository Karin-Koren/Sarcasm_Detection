import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import org.json.JSONObject;
import java.nio.file.Paths;
import software.amazon.awssdk.services.sqs.model.*;

public class Worker {

    final static AWS aws = AWS.getInstance();
    final String BUCKET_NAME = "ks100";
    static SentimentAnalysisHandler sentimentAnalysisHandler = new SentimentAnalysisHandler();
    static NamedEntityRecognitionHandler namedEntityRecognitionHandler = new NamedEntityRecognitionHandler();
    File workFile = new File("workFile");
    File outWorkFile = new File("outWorkFile");
    String workFileName = "workFile";
    String outFileName = "outWorkFile";

    private String queueUrlToWorkers;
    private String queueUrlToManager;

    public Worker() {
        this.queueUrlToWorkers = aws.getQueUrl("ToWorkers");
        this.queueUrlToManager = aws.getQueUrl("ToManager");
    }

    public void processFile(String S3DownPath, String S3UpPath) {

        // 3. download a file from S3 -> according to the msg
        System.out.println("processFile: start download...");
        aws.downloadFileFromS3(BUCKET_NAME, S3DownPath, Paths.get(workFileName));
        // process
        try (BufferedReader reader = new BufferedReader(new FileReader(workFileName));
                FileWriter writer = new FileWriter(outFileName, true)) {
            String line;
            while ((line = reader.readLine()) != null) {
                JSONObject review = new JSONObject(line);
                String text = review.getString("text");
                int stars = review.getInt("stars");
                String link = review.getString("link");
                int sentiment = sentimentAnalysisHandler.findSentiment(text);
                String entities = namedEntityRecognitionHandler.getEntities(text);

                JSONObject parsedReview = new JSONObject();
                parsedReview.put("text", text);
                parsedReview.put("stars", stars);
                parsedReview.put("link", link);
                parsedReview.put("sentiment", sentiment);
                parsedReview.put("entities", entities);

                writer.write(parsedReview.toString() + "\n");
            }
            System.out.println("Parsed reviews written to file: " + outFileName);
        } catch (IOException e) {
            System.err.println("Error reading or writing file: " + e.getMessage());
        }
        // 5. upload the processed files to S3
        aws.uploadFile(BUCKET_NAME, outFileName, S3UpPath);

        // 6. send a msg to the SQS queue -> toManager
        aws.sendMessageToQueue(queueUrlToManager, S3UpPath);

        // delete irrelevent local file content
        try {
            FileWriter writer = new FileWriter(outFileName);
            writer.write("");
            writer.close();
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }

        // delete the file from s3
        aws.deleteFileFromS3(BUCKET_NAME, S3DownPath);
    }

    // Check how the manager should know weather to terminate workers or not
    // Check if the manager or the worker are deciding weather to terminate a worker
    public void listenToQueue() { // THE MANAGER CAN SEND A TERMINATE TASK IN THE QUEUEUE
        while (true) { // when should be terminate just break
            Message msg = aws.receiveMessage(queueUrlToWorkers);
            if (msg != null && !msg.body().equals("terminate")) {
                System.out.println("msg.body: " + msg.body());
                String s3DownPath = msg.body();
                String[] msgArr = s3DownPath.split("/");
                String folderName = msgArr[0] + "/" + msgArr[1];
                String uniqueNum = msgArr[2].split("_")[1];
                String s3UpPath = folderName + "/" + "doneFile_" + uniqueNum;
                this.processFile(s3DownPath, s3UpPath);
                aws.deleteMessage(msg.receiptHandle(), queueUrlToWorkers);

            } else if (msg != null) {
                System.out.println("get msg: " + msg.body());
                aws.deleteMessage(msg.receiptHandle(), queueUrlToWorkers);
                break;
            }
        }
    }
}