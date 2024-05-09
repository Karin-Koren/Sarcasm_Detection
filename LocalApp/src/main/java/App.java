import java.util.LinkedList;
import java.util.List;

public class App {
    final static AWS aws = AWS.getInstance();

    public static void main(String[] args1) {// args = [inFilePath, outFilePath, tasksPerWorker, -t (terminate,
                                             // optional)]

        String[] args = { "input2.txt", "Test2.html", "90", "-t" };

        // String[] args1 = { "input1.txt", "input2.txt", "input3.txt", "input4.txt",
        // "input5.txt", "output1.html",
        // "output2.html", "output3.html", "output4.html", "output5.html", "70", "-t" };

        List<String> in = new LinkedList<>();
        // in.add("input3.txt");
        List<String> out = new LinkedList<>();
        // out.add("Test.html");
        int n;
        boolean terminate = false;
        int index;
        if (args[args.length - 1].equals("-t")) {
            terminate = true;
            n = Integer.parseInt(args[args.length - 2]);
            index = args.length - 3;
        } else {
            n = Integer.parseInt(args[args.length - 1]);
            index = args.length - 2;
        }
        System.out.println("n = " + n);
        for (int i = 0; i < (index + 1) / 2; i++) {
            System.out.println("in:" + args[i]);
            System.out.println("out:" + args[i + ((index + 1) / 2)]);
            in.add(args[i]);
            out.add(args[i + ((index + 1) / 2)]);
        }

        LocalApp l = new LocalApp(in, out, n, terminate);
        l.run();

        System.out.println("done!");

    }
}
