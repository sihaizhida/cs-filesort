package com.eshore.filesort;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;
import java.util.*;


/**
 * Created by Ronald on 2017/1/11.
 */
public class HdfsUtil {
    private static Logger logger = LoggerFactory.getLogger(HdfsUtil.class);

    //搜索文件获取partition
    public static ArrayList<String> getPartitionList() throws IOException{
        ArrayList<String> partitionList = new ArrayList<>();
        for(int i=1;i<90;i++){
            partitionList.add(i+"");
        }
        partitionList.add(10001+"");
        return partitionList;
    }

    public static boolean copyDirectory(String src , String dst) throws Exception{
        FileSystem fs = FileSystem.get(Conf.getConf());
        if(!fs.exists(new Path(dst))){
            fs.mkdirs(new Path(dst));
        }
        FileStatus status = fs.getFileStatus(new Path(dst));
        File file = new File(src);

        if(status.isFile()){
            System.exit(2);
            System.out.println("You put in the "+dst + "is file !");
        }

        File[] files = file.listFiles();
        for(int i = 0 ;i< files.length; i ++){
            File f = files[i];
            if(f.isDirectory()){
                copyDirectory(f.getPath(),dst);
            }else{
                //输出目录/partition/文件
                fs.copyFromLocalFile(new Path(f.getPath()),new Path(dst+"/"+ files[i].getName().split("\\-")[1] + "/" + files[i].getName()));
                //copyFile(f.getPath(),dst+"/"+ files[i].getName().split("\\-")[1] + "/" + files[i].getName());
            }
        }
        return true;
    }

    public static boolean copyFile(String src , String dst) throws Exception{
        FileSystem fs = FileSystem.get(Conf.getConf());
        fs.exists(new Path(dst));
        File file = new File(src);

        InputStream in = new BufferedInputStream(new FileInputStream(file));
        /**
         * FieSystem的create方法可以为文件不存在的父目录进行创建，
         */
//        OutputStream out = fs.create(new Path(dst) , new Progressable() {
//        });
        FSDataOutputStream out = fs.create(new Path(dst),true);
        IOUtils.copyBytes(in, out, 4096, true);
        return true;
    }


    //文件重命名
    public static void rename(File outputpath,Map<String,Long> map,String tableName){
        if(outputpath.exists()){
            if(outputpath.isDirectory()){
                File[] files = outputpath.listFiles();
                if(files.length==0){
                    logger.error(outputpath +"为空目录！");
                    return;
                }
                for(int i=0;i<files.length;i++){
                    if(files[i].getName().startsWith(tableName + "-") && files[i].isFile()){
                        renameFile(files[i],map);
                    }

                    if(files[i].isDirectory()){
                        rename(files[i],map,tableName);
                    }
                }
            }

            if(outputpath.isFile()){
                renameFile(outputpath,map);
            }
        }else{
            logger.error(outputpath + "不存在！");
            return;
        }
    }

    public static void renameFile(File file,Map<String,Long> map){
        String originalName = file.getName();
        String outpath = file.getParent().toString();
        //System.out.println("originalName:" + originalName);
        //文件数据行数
        long num = map.get(originalName.split("\\.")[0]).longValue();
        if(originalName.split("\\-").length!=3){
            logger.error("源文件名格式有误！");
        }else{
            String newName = originalName.split("\\-")[0] + "-" + originalName.split("\\-")[1] + "-" + num + "-" +
                    originalName.split("\\-")[2];
            //System.out.println("newName:" + newName);
            file.renameTo(new File( outpath + "/" + newName));
        }
    }
}
