import java.util.HashMap;
import java.util.Map;

public class ClientData {

    public class RawFileData { // big files

        private String s3Path;
        private int miniFilesCount; // how many mini-files was created -> will minus for every done

        public RawFileData(String s3Path) {
            this.s3Path = s3Path;
            this.miniFilesCount = 0;
        }

        public void setMiniFilesCount(int x) {
            this.miniFilesCount = x;
        }

        public boolean gotDoneFile() {
            this.miniFilesCount--;
            return this.miniFilesCount == 0;
        }

        public int getMiniFilesCount() {
            return this.miniFilesCount;
        }

        public String getS3Path() {
            return this.s3Path;
        }
    }

    private String localId;
    private int n;
    private Map<String, RawFileData> rawFiles;
    private int rawFilesCount; // how many raw-files was created -> will minus for every summery

    public ClientData(String localId, int n, int rawFilesCount) {

        this.localId = localId;
        this.n = n;
        this.rawFilesCount = rawFilesCount;
        this.rawFiles = new HashMap<String, RawFileData>();
        for (int i = 0; i < rawFilesCount; i++) {
            String s3Path = localId + "/" + Integer.toString(i);
            rawFiles.put(s3Path, new RawFileData(s3Path));
        }
    }

    public boolean gotProcessedFile() {
        rawFilesCount--;
        return this.rawFilesCount == 0;
    }

    public String getLocalId() {
        return localId;
    }

    public int getRawFilesCount() {
        return rawFilesCount;
    }

    public Map<String, RawFileData> getRawFiles() {
        return rawFiles;
    }

    public int getN() {
        return n;
    }
}
