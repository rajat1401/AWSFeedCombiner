import javax.xml.transform.*;
import org.w3c.dom.Document;
//import Helper.convertStringToXMLDocument;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.util.*;
import java.io.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;


public class Main implements Runnable{
    public String[] array;
    public int low, high;
    public Stream<String> stream;
    //a storing variable

    public Main(String[] arr, int l, int h){
        array= arr;
        low= l;
        high= h;
    }


    public void run(){
        String str= array[low];
        String filepath= "./src/" + str;
        //read the file
        try{
            GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(str));
            BufferedReader br = new BufferedReader(new InputStreamReader(gzip));
            stream= br.lines();
        }catch(Exception e){
            System.out.println(e);
        }
    }

    public Stream<String> getResult(){
        return stream;
    }


    public static void main(String[] args) throws InterruptedException{
        Scanner sc= new Scanner(System.in);
        int num= sc.nextInt();
        String[] array= new String[num];
        for (int i=0; i<num; i++){
            array[i]= sc.next();
        }
        Main[] container= new Main[num];
        long startTime = System.nanoTime();
        for (int i=0; i<num; i++){
            container[i]= new Main(array, i, i+1);
        }
        Thread[] threads= new Thread[num];
        for (int i=0; i<num; i++){
            threads[i]= new Thread(container[i]);
            threads[i].start();
        }
        for(int i=0; i<num; i++){
            threads[i].join();
        }

        Stream<String> final1= Stream.concat(container[0].getResult(), container[1].getResult());
        Stream<String> final2= Stream.concat(container[2].getResult(), container[3].getResult());
        Stream<String> out= Stream.concat(final1, final2);
        Document doc = Helper.convertStringToXMLDocument(out.collect(Collectors.joining()));
        TransformerFactory tf = TransformerFactory.newInstance();
        Transformer transformer;
        try{
            transformer = tf.newTransformer();
            FileOutputStream outStream = new FileOutputStream(new File("out.xml.gz"));
            transformer.transform(new DOMSource(doc), new StreamResult(outStream));
        }catch (Exception e){
            System.out.println(e);
        }
        long endTime = System.nanoTime();
        System.out.println("Time taken is about " + (endTime-startTime)/1e9 + " seconds.");

    }
}
