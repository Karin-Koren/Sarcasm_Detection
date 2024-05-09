import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.nio.file.Files;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import software.amazon.awssdk.services.sqs.model.*;
import org.json.JSONObject;
import org.json.JSONArray;

public class Manager {

    final static AWS aws = AWS.getInstance();
    final String BUCKET_NAME = "ks100";
    private HashMap<String, ClientData> clientsMap;
    private boolean toTerminate = false;
    private String queueUrlToLocals;
    private String queueUrlLocalsToManager;
    private String queueUrlToManager;
    private String queueUrlToWorkers;
    private Thread localQueueThread;

    private int reviewNum = 0;

    File backUpFile = new File("backUpFile"); // bucketName/manager
    String backUpFileName = "backUpFile";

    File rawWorkFile = new File("rawWorkFile");
    File miniOutFile = new File("miniOutFile");
    String rawWorkFileName = "rawWorkFile";
    String miniOutFileName = "miniOutFile";

    File miniWorkFile = new File("miniWorkFile");
    File summeryOutFile = new File("summeryOutFile");
    String miniWorkFileName = "miniWorkFile";
    String summeryOutFileName = "summeryOutFile";

    // CLASS MAP: LOCALID: n, FILECOUNT,FILE:MINIFILECOUNT, FILENAMELIST
    public Manager() {
        System.out.println("starting manager...");
        // check if BackUp is on and take from there the MAP
        if (aws.doesObjectExist(BUCKET_NAME, "manager/backUp")) {
            System.out.println("backUp_EXISTS");
            aws.downloadFileFromS3(BUCKET_NAME, "manager/backUp", Paths.get(backUpFileName));
            String data = "";
            try (BufferedReader reader = new BufferedReader(new FileReader(backUpFileName))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    data += line;
                }
            } catch (IOException e) {
                System.err.println("Error reading file: " + e.getMessage());
            }
            this.clientsMap = stringToHashMap(data);
        } else {
            // create the MAP
            this.clientsMap = new HashMap<String, ClientData>();
        }
        this.queueUrlLocalsToManager = aws.getQueUrl("LocalsToManager");
        this.queueUrlToLocals = aws.getQueUrl("ManagerToLocals");
        aws.setupBasicQueuePolicy(queueUrlLocalsToManager);
        init();
    }

    public void init() {
        // create ToWorkers sqs
        aws.createSqsQueueWithVisibilityTimeout("ToWorkers");
        this.queueUrlToWorkers = aws.getQueUrl("ToWorkers");
        // create ToManager sqs
        aws.createQueueIfNotExists("ToManager");
        this.queueUrlToManager = aws.getQueUrl("ToManager");
    }

    public void termination() {
        aws.closeQueue(queueUrlLocalsToManager);
    }

    public int parseFile(String downPathKey, String customerDir) {
        System.out.println("in parseFils");

        String LocaliD = customerDir.split("/")[0];
        int reviewsPerWorker = this.clientsMap.get(LocaliD).getN() / 2; // n, num of reviews in a file
        int reviewCounter = 0;
        String currentStr = ""; // parsed review
        int upFileCount = 1; // file name counter
        String upPathKey = "";
        int miniFilesCounter = 0;
        System.out.println("reviewsPerWorker = " + reviewsPerWorker);

        // 3. download a file from S3 -> according to the msg
        System.out.println("start download...");
        aws.downloadFileFromS3(BUCKET_NAME, downPathKey, Paths.get(rawWorkFileName));

        try (BufferedReader reader = new BufferedReader(new FileReader(rawWorkFileName))) {
            String line;
            while ((line = reader.readLine()) != null) {
                JSONObject jsonObject = new JSONObject(line);
                String title = jsonObject.getString("title");
                JSONArray reviews = jsonObject.getJSONArray("reviews");
                // String parsedReview;
                for (int j = 0; j < reviews.length(); j++) {
                    JSONObject review = reviews.getJSONObject(j);
                    JSONObject parsedReview = new JSONObject();
                    parsedReview.put("title", title);
                    parsedReview.put("text", review.getString("text"));
                    parsedReview.put("stars", review.getInt("rating"));
                    parsedReview.put("link", review.getString("link"));

                    currentStr += parsedReview.toString() + "\n";
                    reviewCounter++;

                    if (reviewCounter == reviewsPerWorker) {
                        System.out.println("upload......");
                        Files.write(Paths.get(miniOutFileName), currentStr.getBytes());
                        upPathKey = customerDir + "/" + miniOutFileName + '_' + String.valueOf(upFileCount);
                        upFileCount++;
                        aws.uploadFile(BUCKET_NAME, miniOutFileName, upPathKey);
                        System.out.println("finish upload......");
                        miniFilesCounter++;
                        this.reviewNum += reviewCounter;
                        reviewCounter = 0;
                        currentStr = ""; // Reset for next batch

                        // send the workers the task
                        aws.sendMessageToQueue(queueUrlToWorkers, upPathKey);
                        // delete the file from s3
                        aws.deleteFileFromS3(BUCKET_NAME, downPathKey);
                    }
                }

            }
            if (!currentStr.isEmpty()) {
                System.out.println("upload......");
                Files.write(Paths.get(miniOutFileName), currentStr.getBytes());
                upPathKey = customerDir + "/" + miniOutFileName + '_' + String.valueOf(upFileCount);
                upFileCount++;
                aws.uploadFile(BUCKET_NAME, miniOutFileName, upPathKey);
                System.out.println("finish upload......");
                miniFilesCounter++;
                this.reviewNum += reviewsPerWorker;
                reviewCounter = 0;

                // send the workers the task
                aws.sendMessageToQueue(queueUrlToWorkers, upPathKey);
            }
        } catch (IOException e) {
            System.err.println("Error reading file: " + e.getMessage());
        }
        // delete the file from s3
        aws.deleteFileFromS3(BUCKET_NAME, downPathKey);
        return miniFilesCounter;
    }

    public void parseFileFlow(int n, String localId, String filePath) {
        System.out.println("in parseFileFlow");
        // parse the file
        String cur_dir = filePath.split("/")[0] + "/" + filePath.split("/")[1];
        System.out.println("filePath: " + filePath);
        System.out.println("cur_dir: " + cur_dir);
        int counter = parseFile(filePath, cur_dir); // how many mini-files

        // update each file to MAP
        System.out.println("localId: " + localId);
        System.out.println("map: " + hashMapToString(this.clientsMap));

        this.clientsMap.get(localId).getRawFiles().get(cur_dir).setMiniFilesCount(counter);
        // create workers
        create_workers();

        System.out.println("counter = " + counter);

    }

    public void processRawMsg(String msg) {
        System.out.println("in processRawMsg........");
        System.out.println("msg: " + msg);
        // localId|n|path|path|...|path|terminate-t/f
        String[] msgArray = msg.split("\\|");
        String terminate = msgArray[msgArray.length - 1];
        if (terminate.equals("t")) {
            this.toTerminate = true;
            termination(); // close the que connection
        }
        String localId = msgArray[0];
        int n = Integer.parseInt(msgArray[1]);
        // add a clientData
        ClientData cliData = new ClientData(localId, n, msgArray.length - 3);
        this.clientsMap.put(localId, cliData);
        for (int i = 2; i < msgArray.length - 1; i++) {
            parseFileFlow(n, localId, msgArray[i]);
        }
        // write to backUpfile
        String content = hashMapToString(clientsMap);
        try {
            Files.write(Paths.get(backUpFileName), content.getBytes());
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
        // upload to backUp
        aws.uploadFile(BUCKET_NAME, backUpFileName, "manager/backUp");
    }

    public void listenToLocalQue() {
        System.out.println("in listenToLocalQue");
        // listen to LocalsToManager -> if terminate is on and there is no msg in the
        // que->exit
        while (true) {
            Message msg = aws.receiveMessage(queueUrlLocalsToManager);
            if (msg != null) {
                processRawMsg(msg.body());
                // delete task msg
                aws.deleteMessage(msg.receiptHandle(), queueUrlLocalsToManager);
            } else if (this.toTerminate) {
                break;
            }
        }
    }

    public void appendMiniFile() throws IOException {
        // Create readers and writers
        BufferedReader reader = new BufferedReader(new FileReader(miniWorkFileName));
        BufferedWriter writer = new BufferedWriter(new FileWriter(summeryOutFileName,
                true));
        try {
            // Read line by line from source file and write to destination file
            String line;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine(); // Add new line separator
            }
        } finally {
            // Close readers and writers
            if (reader != null) {
                reader.close();
            }
            if (writer != null) {
                writer.close();
            }
        }
    }

    public void create_workers() {
        // create workers
        int workersNumber = aws.getNumOfInstancesCount("Worker");
        int nAvg = 0;
        for (String cdKey : this.clientsMap.keySet()) {
            ClientData cd = this.clientsMap.get(cdKey);
            nAvg += cd.getN();
        }
        if (this.clientsMap.keySet().size() == 0) {
            return;
        }
        nAvg = nAvg / this.clientsMap.keySet().size();
        int wantedWorkers = this.reviewNum / nAvg;
        if (this.reviewNum % nAvg != 0)
            wantedWorkers++;
        if (workersNumber < wantedWorkers) {
            for (int i = 0; i < wantedWorkers - workersNumber; i++) {
                System.out.println("create worker - " + i);
                if (aws.getNumOfInstancesCount("Worker") < 8) {
                    String workerScript = "#!/bin/bash\n" +
                            "sudo yum update -y\n" +
                            "sudo yum install ec2-instance-connect\n" +
                            "sudo yum install -y java-21-amazon-corretto\n" +
                            "sudo wget https://ks100.s3.us-west-2.amazonaws.com/SetUp/Worker.jar -O Worker.jar\n" +
                            "sudo java -Xms4g -Xmx4g -jar Worker.jar\n" +
                            "shutdown -h now";
                    aws.createEc2(1, 1, "Worker", workerScript);
                }
            }
        }
    }

    public void create_summary(String localId, String dirPath) {
        System.out.println("in create_summary");
        // download each file and append to SummeryFile
        System.out.println("dirPath " + dirPath);
        List<String> fileNames = aws.listS3Objects(BUCKET_NAME, dirPath + "/");
        System.out.println("dirPath " + fileNames.get(0));
        this.reviewNum -= fileNames.size() * (this.clientsMap.get(localId).getN() / 2);
        for (String fileName : fileNames) {
            System.out.println("fileName: " + fileName);
            aws.downloadFileFromS3(BUCKET_NAME, fileName, Paths.get(miniWorkFileName));
            try {
                appendMiniFile();
            } catch (Exception e) {
                System.out.println("error");
            }
        }
        // upload file to S3
        aws.uploadFile(BUCKET_NAME, summeryOutFileName, dirPath + "/summery");
        // send msg to ToLocals
        String msg = localId + "|" + dirPath + "/summery";
        System.out.println("localmsg: " + msg);
        aws.sendMessageToQueue(queueUrlToLocals, msg);
        // delete mini-files from s3
        for (String fileName : fileNames) {
            aws.deleteFileFromS3(BUCKET_NAME, fileName);
        }
        // delete local files content
        try {
            // delete irrelevent summery content
            FileWriter writer = new FileWriter(summeryOutFileName);
            writer.write("");
            writer.close();
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
        // update MAP
        boolean is_last = this.clientsMap.get(localId).gotProcessedFile();
        System.out.println("clienmap: " + hashMapToString(this.clientsMap));

        if (is_last) {
            this.clientsMap.remove(localId); // done work with this local->last RawFile processed
            // check how many workers should run
            int workersNumber = aws.getNumOfInstancesCount("Worker");
            int nAvg = 0;
            for (String cdKey : this.clientsMap.keySet()) {
                ClientData cd = this.clientsMap.get(cdKey);
                nAvg += cd.getN();
            }
            int wantedWorkers;
            if (this.clientsMap.keySet().size() != 0) {
                nAvg = nAvg / this.clientsMap.keySet().size();
                wantedWorkers = this.reviewNum / nAvg;
                if (this.reviewNum % nAvg != 0)
                    wantedWorkers++;
            } else
                wantedWorkers = 0;
            // remove workersNumber-wantedWorkers
            for (int i = 0; i < (workersNumber - wantedWorkers) && i < 8; i++) {
                // send terminate msg to workers queue
                aws.sendMessageToQueue(queueUrlToWorkers, "terminate");
            }
            // double check worker creations
            create_workers();
        }
    }

    public void miniFileMsg(String msg) {

        // process the msg
        String[] msgArr = msg.split("/"); // path: localId/indexFile/fileName
        String localId = msgArr[0];
        // update the MAP
        String dirPath = msgArr[0] + "/" + msgArr[1];
        boolean is_last = this.clientsMap.get(localId).getRawFiles().get(dirPath)
                .gotDoneFile();
        // when we get the last mini file well go to the MAP and aggregate all the files
        if (is_last) {
            create_summary(localId, dirPath);
        }
        // write to backUpfile
        String content = hashMapToString(clientsMap);
        try {
            Files.write(Paths.get(backUpFileName), content.getBytes());
        } catch (Exception e) {
            System.out.println("Error: " + e);
        }
        // upload to backUp
        aws.uploadFile(BUCKET_NAME, backUpFileName, "manager/backUp");
    }

    public void listenWorkersToManagerQue() {
        // listen to ToManager -> we get mini files need to aggregate
        System.out.println("in listenWorkersToManagerQue");
        while (true) {
            Message msg = aws.receiveMessage(queueUrlToManager);
            if (msg != null) {
                miniFileMsg(msg.body());
                // delete task msg
                aws.deleteMessage(msg.receiptHandle(), queueUrlToManager);
            } else if (this.toTerminate && this.clientsMap.isEmpty()) {
                // and if there are no clients in MAP
                aws.deleteFileFromS3(BUCKET_NAME, "manager/backUp");
                aws.deleteQueue(queueUrlLocalsToManager);
                aws.deleteQueue(queueUrlToLocals);
                aws.deleteQueue(queueUrlToManager);
                aws.deleteQueue(queueUrlToWorkers);
                try {
                    localQueueThread.join();
                } catch (InterruptedException e) {
                    System.err.println("Error: " + e.getMessage());
                }
                aws.close();
                break;
            }
        }
    }

    public void runManagerFlow() {
        System.out.println("starting flow...");
        localQueueThread = new Thread(this::listenToLocalQue);
        localQueueThread.start();
        listenWorkersToManagerQue();
    }

    // Converts a HashMap to a JSON String
    public String hashMapToString(HashMap<String, ClientData> map) {
        Gson gson = new Gson();
        return gson.toJson(map);
    }

    // Converts a JSON String back to a HashMap
    public HashMap<String, ClientData> stringToHashMap(String json) {
        Gson gson = new Gson();
        Type type = new TypeToken<HashMap<String, ClientData>>() {
        }.getType();
        return gson.fromJson(json, type);
    }

}