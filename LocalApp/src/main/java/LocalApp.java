import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import software.amazon.awssdk.services.sqs.model.*;
import org.json.JSONObject;

public class LocalApp {

    final String BUCKET_NAME = "ks100";
    private List<String> inputFiles;
    private List<String> outputFiles;
    private int n;
    private int responseCounter;
    private boolean t;
    final static AWS aws = AWS.getInstance();
    private String queueUrlToLocals;
    private String queueUrlLocalsToManager;
    private String appId = "client" + (new Random()).nextInt(10000) + System.currentTimeMillis();

    private File curSumFile = new File("curSumFile");
    private String curSumFileName = "curSumFile";

    String managerScript;

    LocalApp(List<String> inputFiles, List<String> outputFiles, int n, boolean t) {
        this.inputFiles = inputFiles;
        this.outputFiles = outputFiles;
        this.n = n;
        this.t = t;
        responseCounter = inputFiles.size();
        System.out.println("ID = " + appId);
        init();
    }

    public void init() {
        // call create manager if not exist
        managerScript = "#!/bin/bash\n" +
                "sudo yum update -y\n" +
                "sudo yum install ec2-instance-connect\n" +
                "sudo yum install -y java-21-amazon-corretto\n" +
                "sudo wget https://ks100.s3.us-west-2.amazonaws.com/SetUp/Manager.jar -O Manager.jar\n" +
                "sudo java -jar Manager.jar\n" +
                "shutdown -h now";

        aws.createManagerIfNotExists(managerScript);
        // call create bucket if not exist
        aws.createBucketIfNotExists(BUCKET_NAME);
        // upload files to s3
        int counter = 0;
        for (String fileName : inputFiles) {
            String filePath = appId + "/" + Integer.toString(counter) + "/" + fileName;
            aws.uploadFile(BUCKET_NAME, fileName, filePath);
            counter++;
        }
    }

    public String createMessage() {
        // localId|n|path|path|...|path|terminate-t/f
        String msg = this.appId + "|" + this.n;
        int counter = 0;
        for (String fileName : inputFiles) {
            String filePath = appId + "/" + Integer.toString(counter) + "/" + fileName;
            msg += "|" + filePath;
            counter++;
        }
        if (this.t)
            msg += "|t";
        else
            msg += "|f";
        return msg;
    }

    public boolean isMine(String body) {
        return body.split("\\|")[0].equals(this.appId);
    }

    public void processMsg(String body) {
        // manger msg: appId|s3DownloadPath
        String s3DownloadPath = body.split("\\|")[1];
        int indexOutFile = Integer.parseInt(s3DownloadPath.split("/")[1]); // PATH: ID/INDEX/FILE
        // download output files from s3
        aws.downloadFileFromS3(BUCKET_NAME, s3DownloadPath, Paths.get(curSumFileName));
        // Create an html file representing the results
        createHtmlFile(indexOutFile);
        // delete summery file
        aws.deleteFileFromS3(BUCKET_NAME, s3DownloadPath);
    }

    public boolean sarcasmDetect(int stars, int sentiment) {
        return (((sentiment == 1 || sentiment == 0) && stars > 3) || (sentiment == 4 || sentiment == 3) && stars < 3);
    }

    public void createHtmlFile(int indexOutFile) {

        String htmlContent = "<html><body>";
        try (BufferedReader reader = new BufferedReader(new FileReader(curSumFileName))) { // curSumFileName
            String line;
            while ((line = reader.readLine()) != null) {
                JSONObject review = new JSONObject(line);
                // String title = jsonObject.getString("title");
                int stars = review.getInt("stars");
                String link = review.getString("link");
                int sentiment = review.getInt("sentiment");
                HashMap<Integer, String> sentimentDict = new HashMap<>();
                sentimentDict.put(0, "darkred");
                sentimentDict.put(1, "red");
                sentimentDict.put(2, "black");
                sentimentDict.put(3, "lightgreen");
                sentimentDict.put(4, "darkgreen");
                String entities = review.getString("entities");

                htmlContent += "<p><a href=\"" + link + "\" style=\"color:" + sentimentDict.get(sentiment) + ";\">"
                        + link
                        + "</a> ";
                htmlContent += entities;
                if (sarcasmDetect(stars, sentiment))
                    htmlContent += " SARCASTIC!</p><br>";
                else
                    htmlContent += " NOT SARCASTIC</p><br>";
            }
            // Files.write(Paths.get(outFileName), currentStr.getBytes());

        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }

        htmlContent += "</body></html>";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFiles.get(indexOutFile)))) {
            // Write the HTML content to the file
            writer.write(htmlContent);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run() {
        System.out.println("Starting run ....");
        aws.createQueueIfNotExists("LocalsToManager");
        this.queueUrlLocalsToManager = aws.getQueUrl("LocalsToManager");

        aws.createQueueIfNotExists("ManagerToLocals");
        this.queueUrlToLocals = aws.getQueUrl("ManagerToLocals");
        System.out.println("create Queue ....");

        // update manager about files in sqs
        aws.sendMessageToQueue(createMessage(), queueUrlLocalsToManager);
        System.out.println("send Message....");

        // listen to sqs manager if manager is done
        while (true) {
            // check if manager is alive - and wake it up if not
            aws.createManagerIfNotExists(managerScript);
            // when should be terminate just break
            Message msg = aws.receiveMessage(queueUrlToLocals);
            if (msg != null && isMine(msg.body())) {
                responseCounter--;
                processMsg(msg.body());
                aws.deleteMessage(msg.receiptHandle(), queueUrlToLocals);
                if (responseCounter == 0)
                    break;
            }

            try {
                Thread.sleep(10000); // Sleep for 10 seconds (10000 milliseconds)
            } catch (InterruptedException e) {
                System.out.println("error: " + e);
            }
        }
    }
}
