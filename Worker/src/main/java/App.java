
public class App {

    final static AWS aws = AWS.getInstance();

    public static void main(String[] args) {
        System.out.println("Start");
        Worker worker = new Worker();
        System.out.println("Start1");

        // worker.processFile("client49841708000089225/0/miniOutFile_16",
        // "client49841708000089225/0/test16");
        worker.listenToQueue();
        // System.out.println("done");

        // String path = "C:\\Users\\karin\\Desktop\\AWS -
        // ass1\\Worker\\target\\Worker.jar";
        // aws.uploadFile("ks100", path, "SetUp/Worker.jar");

    }
}
