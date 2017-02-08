package com.eshore.filesort;

import com.eshore.ecache.dao.EntityDAOException;
import com.eshore.ecache.schema.Entity;
import com.eshore.ecache.tools.HashBucket;

/**
 * Created by Ronald on 2017/1/13.
 */
public class EntityUtil {

    public static int getBucketNum(Entity entity){
        return entity.getBucketNumber();
    }

    public static int getBucketIndex(Entity entity, String inputFormat){
        String bucketKey = entity.getBucketKey().toString().split(":")[0];
        //System.out.println("bucketKey:" + bucketKey);
        String[] inputFormatArray = inputFormat.split("\\$\\$");
        int index = -1;
        for(int i=0;i<inputFormatArray.length; i++){
            if(bucketKey.equalsIgnoreCase(inputFormatArray[i]))index = i;
        }
        return index;
    }

    public static int getBucketId(Entity entity,Object object) throws EntityDAOException{
        return new HashBucket(getBucketNum(entity)).bucket(object);
    }


}
