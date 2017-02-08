package com.eshore.filesort;
import com.eshore.ecache.dao.EntityDAOException;
import com.eshore.ecache.schema.Entity;
import com.eshore.ecache.schema.Schema;
import com.google.common.io.Files;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by Ronald on 2017/1/14.
 */
class ReadHdfsThread implements Runnable {

    String file;
    String inputFormat;
    String tableName;
    String outputpath;
    ConcurrentMap<String,ArrayList<String>> datamap;
    private static ConcurrentMap<String,Long> counter;

    public ReadHdfsThread(String file,String inputFormat,String tableName,String outputpath,ConcurrentMap<String,ArrayList<String>> datamap,ConcurrentMap<String,Long> counter){
        this.file = file;
        this.inputFormat = inputFormat;
        this.tableName = tableName;
        this.outputpath = outputpath;
        this.datamap = datamap;
        this.counter = counter;
    }

    public void run(){
        int writeBatch = Integer.parseInt(Conf.getConf().get("writeBatch"));
        int writePoolSize = Integer.parseInt(Conf.getConf().get("writePoolSize"));
        //线程池
        ExecutorService executorService = Executors.newFixedThreadPool(writePoolSize);
        BufferedReader in = null;
        ArrayList<String> list = null;
        long starTime = System.currentTimeMillis();
        try{
            FileSystem fs = FileSystem.get(Conf.getConf());
            Path path = new Path(file);
            FSDataInputStream fin = fs.open(path);
            in = new BufferedReader(new InputStreamReader(fin,"UTF-8"));
            System.out.println("file:" + file);
            //表信息
            Entity entity = Schema.getInstance().getEntity(tableName.startsWith("filter_")?tableName.substring(7):tableName);
            int bucketIndex = EntityUtil.getBucketIndex(entity,inputFormat);
            String parAndBuck;
            String line;
            String partition_id;
            int bucket_id;
            while((line=in.readLine())!=null){
                String[] data = line.split("\\$\\$");
                partition_id = data[data.length - 1];
                bucket_id = EntityUtil.getBucketId(entity,data[bucketIndex]);
                parAndBuck = partition_id + "-" + bucket_id;
                line = line + "$$" +bucket_id;
                Long num = 0L;

                synchronized (counter){
                    //将数据放到map里
                    if(!datamap.containsKey(parAndBuck)){
                        String destdir = outputpath + "/" + partition_id + "/"  + tableName + "-" + partition_id + "-" + bucket_id + ".dat";
                        File destpath =  new File(destdir);
                        Files.createParentDirs(destpath);
                        Files.write(inputFormat + "$$bucket_id" + "\n",destpath,Charset.forName("UTF-8"));
                        list = new ArrayList<>();
                        list.add(line);
                        num++;
                        counter.put(tableName + "-" + parAndBuck,Long.valueOf(num));
                        datamap.put(parAndBuck,list);
                    }else{
                        list = datamap.get(parAndBuck);
                        list.add(line);
                        num = counter.get(tableName + "-" + parAndBuck);
                        num++;
                        counter.put(tableName + "-" + parAndBuck,Long.valueOf(num));
                    }

                    if(list.size() >= writeBatch){
                        executorService.submit(new WriteToLocalThread(list,new File(outputpath + "/" + partition_id + "/"  + tableName + "-" + partition_id + "-" + bucket_id + ".dat")));
                        list = new ArrayList<>();
                        datamap.put(parAndBuck,list);
                    }
                }
            }

            Iterator<String> iterator = datamap.keySet().iterator();
            while(iterator.hasNext()){
                String key = iterator.next();
                String partition = key.split("-")[0];
                String bucket = key.split("-")[1];
                list = datamap.get(key);
                if(list.size()>0){
                    executorService.submit(new WriteToLocalThread(list,new File(outputpath + "/" + partition + "/" + tableName + "-" + partition + "-" + bucket + ".dat")));
                    list = new ArrayList<>();
                    datamap.put(key,list);
                }
            }
        }catch (EntityDAOException | IOException e){
            e.printStackTrace();
        }finally {
            try{
                in.close();
            }catch (IOException e){
                e.printStackTrace();
            }
            executorService.shutdown();
            try{
                executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);
                System.out.println("这个读线程耗时：" + (System.currentTimeMillis()-starTime)/1000 + "秒！") ;
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
    }
}
