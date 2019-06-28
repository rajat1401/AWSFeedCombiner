package helloworld;

import com.amazonaws.services.s3.AmazonS3;

import java.io.*;
import java.util.Arrays;
import java.util.List;


class InputStreamIterator{
    public InputStream stream;
    public int bufferSize;
    public byte[] arr;
    public int data;

    public InputStreamIterator(InputStream is, int buffer){
        stream= is;
        bufferSize= buffer;
        arr= new byte[bufferSize];
        try {
            data = stream.read(arr, 0, bufferSize);
        }catch (Exception e){
            System.out.println(e);
        }
    }

    public boolean hasNext() {
        return data!= -1;
    }

    public byte[] next() {
        if(!hasNext()){
            try {
                stream.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        int a = data;
        byte[] temp= Arrays.copyOfRange(arr, 0, a);
        try {
            data = readArray(arr, 0, bufferSize);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return temp;
    }

    public int readArray(byte[] arr, int off, int len){
        byte[] array= new byte[1];
        if (arr == null) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > arr.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        int i= 1;
        try {
            int c = stream.read(array);
            if (c == -1) {
                return -1;
            }
            arr[off] = (byte)c;

            for (; i < len ; i++) {
                c = stream.read(array);
                if (c == -1) {
                    break;
                }
                arr[off + i] = (byte)c;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return i;
    }
}


public class S3Iterator extends InputStream{
    public List<String> nameList;
    public List<Integer> sizeList;
    public InputStream streamList;
    public AmazonS3 client;
    public Long currentBytesServed;
    public File outputFile;
    public int currentFileIndex;
    public int bufferSize;

    public S3Iterator(List<String> names, List<Integer> sizes, InputStream stream, AmazonS3 cli, File opFile){
        nameList= names;
        sizeList= sizes;
        streamList= stream;
        currentFileIndex= 0;
        currentBytesServed= 0L;
        bufferSize= 8000000;
        client= cli;
        outputFile= opFile;
    }


    public Boolean hasNext(){
        if(currentFileIndex>= nameList.size()){
            return false;
        }
        return true;
    }

    public InputStreamIterator startServingNextFile(){
        InputStreamIterator current= null;
        try {
            if (hasNext()) {
                String str= nameList.get(currentFileIndex);
                if(str.substring(str.length()-2).equals("gz")){//might not work for .gz files.
                    streamList= client.getObject("rajat-lamda-edge-test", str).getObjectContent();
                }else {
                    streamList= client.getObject("rajat-lamda-edge-test", str).getObjectContent();
                }
                sizeList.add(streamList.available());
                System.out.println("Size of " + nameList.get(currentFileIndex) + " is " + sizeList.get(currentFileIndex));
                current= new InputStreamIterator(streamList, bufferSize);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return current;
    }

    public void next(){
        try {
            InputStreamIterator currentIterator = startServingNextFile();
            while (currentIterator.hasNext()) {
                byte[] arr= currentIterator.next();
                System.out.println("Here we go again for " + nameList.get(currentFileIndex));
                (new FileOutputStream(outputFile, true)).write(arr);
                System.out.println(arr);
            }
            currentBytesServed+= sizeList.get(currentFileIndex);
            currentFileIndex++;
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public int read() throws IOException {
        while (hasNext()){
            next();
        }
        return 0;
    }
}
