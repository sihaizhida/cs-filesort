package com.eshore.filesort;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 * Created by Ronald on 2017/1/21.
 */
public class WriteToLocalThread implements Runnable{
    private List<String> data;
    private File file;

    public WriteToLocalThread(List<String> data,File file){
        super();
        this.data = data;
        this.file = file;
    }

    public void run(){
        synchronized (file){
            try {
                StringBuffer stringBuffer = new StringBuffer();
                for (String string : data) {
                    stringBuffer.append(string + "\n");
                }
                Files.append(stringBuffer.toString(), file, Charset.forName("UTF-8"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
