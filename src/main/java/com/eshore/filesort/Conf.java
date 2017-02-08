package com.eshore.filesort;

import org.apache.hadoop.conf.Configuration;
/***
 * 初始化hadoop、hdfs配置文件，以及缓存服务自定义配置
 * @author lijd
 *
 */
public class Conf {

        private static Configuration conf;
        static {
            Configuration.addDefaultResource("core-site.xml");		//hadoop配置文件
            Configuration.addDefaultResource("hdfs-site.xml");		//hdfs配置文件
            Configuration.addDefaultResource("ecache-site.xml");	//缓存服务自定义配置
            conf = new Configuration();
        }

        public static Configuration getConf() {
            return conf;
        }

        public static String get(String name) {
            return conf.get(name);
        }

        public static String get(String name, String defaultValue) {
            return conf.get(name, defaultValue);
        }


}
