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
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;


class Appender implements Runnable{
    public String[] array;
    public int current;
    public Document papaDoc;
    public NodeList papaList;

    public Appender(String[] arr, Document doc, NodeList papal, int i){
        array= arr;
        current= i;
        papaDoc= doc;
        papaList= papal;
    }

    public void run(){
        String currentpath= "./src/" + array[current];
        try{
            Document currentDoc;
            if(currentpath.substring(currentpath.length()-2).equals("gz")){
                System.out.println("YAHOO");
                GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(currentpath));
                BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
                currentDoc= Helper.convertStringToXMLDocument(br.lines().collect(Collectors.joining()));
            }else{
                DocumentBuilderFactory domFactory = DocumentBuilderFactory.newInstance();
                domFactory.setIgnoringComments(true);
                DocumentBuilder builder = domFactory.newDocumentBuilder();
                currentDoc= builder.parse(new File(currentpath));
            }
            NodeList currentList= currentDoc.getElementsByTagName("job");
            System.out.println(currentpath + "\t" + currentList.getLength());
            Node PARENT= papaList.item(papaList.getLength()-1).getParentNode();
            for(int i=0; i<currentList.getLength(); i++){
                Node n= (Node) papaDoc.importNode(currentList.item(i), true);
                PARENT.appendChild(n);
            }
            System.out.println("After " + currentpath + ": " + papaList.getLength());
        }catch(Exception e){
            System.out.println(e);
        }
    }
}


//CAN MAKE THE WAIT TIME IN AWAIT TERMINATION DYNAMIC BY REFERENCING THE SIZE OF THE LARGEST FILE.
public class Main{
    public static NodeList papaList;

    public static void main(String[] args) throws InterruptedException{
        int num= 4;
        long[] sizes= new long[num];
        long max= 0;
        int index= -1;
        String[] array= new String[num];
        array[0]= "05eda891-774e-4086-a72a-67b07f4c2db8.xml";
        array[1]= "5ced6337-6e51-4d0d-811a-b79de53b3501.xml";
        array[2]= "6eb1b51e-f7d9-4d00-92c5-9833385fef48.xml.gz";
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
            papaList= papaDoc.getElementsByTagName("job");
            System.out.println("Before: " + papaDoc.getElementsByTagName("job").getLength());
            ExecutorService threadPool= Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() + 1);

            Appender[] container= new Appender[num-1];
            long startTime= System.nanoTime();
            int count= 0;
            for(int i=0; i<num; i++){
                if(i!= index){
                    container[count]= new Appender(array, papaDoc, papaList, i);
                    count+= 1;
                }
            }
            for(int i=0; i<num-1; i++){
                threadPool.execute(container[i]);
            }
            if(!threadPool.isTerminated()){
                threadPool.shutdown();
                threadPool.awaitTermination(10L, TimeUnit.SECONDS);
            }
            TransformerFactory tf = TransformerFactory.newInstance();
            Transformer transformer = tf.newTransformer();
            System.out.println("After: " + papaList.getLength());
            GZIPOutputStream outStream = new GZIPOutputStream(new FileOutputStream(new File("mergedxml.xml")));//do gzipoutput stream here and mergexml.gz
            transformer.transform(new DOMSource(papaDoc), new StreamResult(outStream));
            System.out.println("Merge Complete");

            long endTime = System.nanoTime();
            System.out.println("Time taken is about " + (endTime-startTime)/1e9 + " seconds.");
        }catch(Exception e){
            System.out.println(e);
        }
    }
}
