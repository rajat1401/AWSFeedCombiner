import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.*;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.*;
import java.util.concurrent.*;
import java.util.zip.GZIPOutputStream;


class Appender implements Runnable{
    public String[] array;
    public int current, papa;
    public Document papaDoc;
    public NodeList papaList;

    public Appender(String[] arr, Document doc, NodeList papal, int i, int papaindex){
        array= arr;
        current= i;
        papaDoc= doc;
        papaList= papal;
        papa= papaindex;
    }

    public void run(){
        String currentpath= "./src/" + array[current];
        String papaPath= "./src/" + array[papa];
        try{//put a check here for xml.gz files.
//            GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(filepath));
//            BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
            DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
            domFactory.setIgnoringComments(true);
            DocumentBuilder builder = domFactory.newDocumentBuilder();
            Document currentDoc= builder.parse(new File(currentpath));
            System.out.println("During: " + papaList.getLength());
            NodeList currentList= currentDoc.getElementsByTagName("job");
            for(int i=0; i<currentList.getLength(); i++){
                Node n= (Node) papaDoc.importNode(currentList.item(i), true);
                papaList.item(i).getParentNode().appendChild(n);
            }
        }catch(Exception e){
            System.out.println(e);
        }
    }

    public void getResult(){
        return;
    }
}


public class Main{
    public static NodeList papa;

    public static void main(String[] args) throws InterruptedException{
        int num= 4;
        long[] sizes= new long[num];
        long max= 0;
        int index= -1;
        String[] array= new String[num];
        array[0]= "05eda891-774e-4086-a72a-67b07f4c2db8.xml";
        array[1]= "5ced6337-6e51-4d0d-811a-b79de53b3501.xml";
        array[2]= "6eb1b51e-f7d9-4d00-92c5-9833385fef48.xml";
        array[3]= "8e6f27e2-7116-476c-996c-31ba243f4bce.xml";

        for (int i=0; i<num; i++){
          long a= new File("./src/" + array[i]).length();
          System.out.println(a);
          sizes[i]= a;
        }
        for (int i=0; i<num; i++){
            if(sizes[i]> max){
                max= sizes[i];
                index= i;
            }
        }
        System.out.println(index);
        try{
            DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
            domFactory.setIgnoringComments(true);
            DocumentBuilder builder = domFactory.newDocumentBuilder();
            String papaPath= "./src/" + array[index];
            Document papaDoc= builder.parse(papaPath);
            papa= papaDoc.getElementsByTagName("job");
            System.out.println("Before: " + papaDoc.getElementsByTagName("job").getLength());
            ExecutorService threadPool= Executors.newFixedThreadPool(5);

            Appender[] container= new Appender[num-1];
            long startTime= System.nanoTime();
            int count= 0;
            for(int i=0; i<num; i++){
                if(i!= index){
                    container[count]= new Appender(array, papaDoc, papa, i, index);
                    count+= 1;
                }
            }
            for(int i=0; i<num-1; i++){
                threadPool.execute(container[i]);
            }
            if(!threadPool.isTerminated()){
                threadPool.shutdown();
                threadPool.awaitTermination(3L, TimeUnit.SECONDS);
            }
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            GZIPOutputStream outStream = new GZIPOutputStream(new FileOutputStream(new File("mergedxml.xml.gz")));//do gzipoutput stream here in the final draft
            transformer.transform(new DOMSource(papaDoc), new StreamResult(outStream));
            System.out.println("merge complete");

            long endTime = System.nanoTime();
            System.out.println("Time taken is about " + (endTime-startTime)/1e9 + " seconds.");
        }catch(Exception e){
            System.out.println(e);
        }
    }
}
