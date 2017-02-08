package com.eshore.filesort;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.*;

/*
    hadoop jar cs-filesort-1.0.0-jar-with-dependencies.jar
    inputFormat=check_code$$ACCOUNT_ID$$CUST_ID$$DCS_STATUS_CD$$STATUS_DATE$$AREA_ID$$REC_UPDATE_DATE$$AUDIT_TIME$$FILTER_FLAG$$partition_id
    inputpath=/data
    outputpath=/home/ronald/demo
    tableName=account
     */
public class FileSort {
    private static final Logger logger = LoggerFactory.getLogger(FileSort.class);
    public static ConcurrentMap<String,ArrayList<String>> datamap = new ConcurrentHashMap<>();
    public static ConcurrentMap<String,Long> counter = new ConcurrentHashMap<>();
	public static void main(String[] args) throws Exception {
        if(args.length < 4){
            logger.error("Usage:<input> <output> <tableName>");
        }
        Map<String,String> parameters = new HashMap<>();
        for(int i = 0;i < args.length; i++){
            String key = args[i].split("=")[0];
            String value = args[i].split("=")[1];
            parameters.put(key,value);
        }

        String inputFormat = parameters.get("inputFormat").toString();
        String inputpath = parameters.get("inputpath").toString();
        String outputpath = parameters.get("outputpath").toString();
        String tableName = parameters.get("tableName").toString();
        String outhdfspath = parameters.get("outhdfspath").toString();
        String writeBatch = parameters.get("writeBatch").toString();
        String writePoolSize = parameters.get("writePoolSize").toString();
        System.out.println("inputFormat:" + inputFormat);
        System.out.println("inputpath:" + inputpath);
        System.out.println("outputpath:" + outputpath);
        System.out.println("tableName:" + tableName);
        System.out.println("outhdfspath:" + outhdfspath);
        System.out.println("writeBatch:" + writeBatch);
        System.out.println("writePoolSize:" + writePoolSize);
        Conf.getConf().set("writeBatch",writeBatch);
        Conf.getConf().set("writePoolSize",writePoolSize);

        long startTime = System.currentTimeMillis();
        FileSystem fs = FileSystem.get(Conf.getConf());
        FileStatus[] fileStatuses = fs.listStatus(new Path(inputpath));
        //读线程池
        ExecutorService readexec = Executors.newCachedThreadPool();
        for(int i=0;i< fileStatuses.length;i++){
            System.out.println("Name:" + fileStatuses[i].getPath().getName());
            readexec.execute(new ReadHdfsThread(fileStatuses[i].getPath().toString(),inputFormat,tableName,outputpath,datamap,counter));
        }
        readexec.shutdown();
        readexec.awaitTermination(Long.MAX_VALUE, TimeUnit.HOURS);

        System.out.println("读写文件总耗时：" + (System.currentTimeMillis()-startTime)/1000 + "秒") ;
        HdfsUtil.rename(new File(outputpath),counter,tableName);

        Iterator iterator = counter.keySet().iterator();
        int sum = 0;
        while(iterator.hasNext()){
            String key = iterator.next().toString();
            sum += counter.get(key).longValue();
        }
        System.out.println(tableName +"总共有：" + sum + "条数据！");

        long currentTime = System.currentTimeMillis();
        HdfsUtil.copyDirectory(outputpath,outhdfspath);
        System.out.println("上传文件耗时："+ (System.currentTimeMillis()-currentTime)/1000 + "秒！");
        System.out.println("总耗时：" + (System.currentTimeMillis()-startTime)/1000 + "秒") ;
        System.exit(0);
	 }

}
