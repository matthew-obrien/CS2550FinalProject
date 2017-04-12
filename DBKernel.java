
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

public abstract class DBKernel {

    protected Thread t;
    protected String threadName;
    
    protected ArrayList<File> findOnlyFiles(String sDir) {
        File folder = new File(sDir);
        ArrayList<File> listOfFiles = new ArrayList<>(Arrays.asList(folder.listFiles()));

        for (Iterator<File> i = listOfFiles.iterator(); i.hasNext();) {
            File file = i.next();
            if (file.isFile()) {
                System.out.println("File " + file.getName());
            } else if (file.isDirectory()) {
                i.remove();
                System.out.println("Found a directory " + file.getName());
            }
        }

        return listOfFiles;
    }
}
