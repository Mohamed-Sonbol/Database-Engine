import java.io.*;
import java.util.Vector;

public class Bucket implements java.io.Serializable{

    Vector<RecordReference> recordReferences = new Vector<>(5);
    String path;
    private static final long serialVersionUID = 7526472295622776147L;

    public void saveBucket(String strTableName, int[] indices) throws IOException {
        String indexString = "";
        for(int i = 0; i<indices.length;i++)
            indexString += indices[i] + "";
        try {
            File bucket = new File("src/main/resources/data/buckets/" + strTableName + "_bucket_" + indexString + ".ser");
            if(!bucket.exists()) {
                bucket.createNewFile();
            }
            FileOutputStream fileOut = new FileOutputStream(bucket);
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(this);
            out.close();
            fileOut.close();
            path = "src/main/resources/data/buckets/" + strTableName + "_bucket_" + indexString + ".ser";
        } catch (IOException i) {
            i.printStackTrace();
        }
    }

    public static Bucket loadBucket(String path) {
        try {
            FileInputStream fileInputStream = new FileInputStream(path);
            ObjectInputStream inputStream = new ObjectInputStream(fileInputStream);
            Bucket bucket = (Bucket) inputStream.readObject();
            inputStream.close();
            fileInputStream.close();
            return bucket;
        }
        catch (IOException | ClassNotFoundException i) {
            i.printStackTrace();
        }

        return null;
    }

}
