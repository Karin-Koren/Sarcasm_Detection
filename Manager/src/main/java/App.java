
public class App {
    final static AWS aws = AWS.getInstance();

    public static void main(String[] args) {
        Manager manager = new Manager();
        manager.runManagerFlow();

        // aws.createEC2("null", "Manager", 1);
        // aws.createManagerIfNotExists();
        // int workersNumber = aws.getNumOfInstancesCount("Worker");
        // System.out.println(workersNumber);
        // aws.createEc2(1, 1, "worker", "workerScript");
        // workersNumber = aws.getWorkerInstancesCount();
        // System.out.println(workersNumber);
        // String path = "C:\\Users\\karin\\Desktop\\AWS -
        // ass1\\Manager\\target\\Manager.jar";
        // aws.uploadFile("ks100", path, "SetUp/Manager.jar");
    }
}
