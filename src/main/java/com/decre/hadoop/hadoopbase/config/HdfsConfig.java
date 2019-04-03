package com.decre.hadoop.hadoopbase.config;

import com.decre.hadoop.hadoopbase.service.HdfsService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * @author Decre
 * @date 2019/4/2 0002 0:00
 * @since 1.0.0
 * Descirption:
 */
@Configuration
public class HdfsConfig {

    @Value("${hdfs.defaultFS}")
    private String defaultHdfsUri;

    @Bean
    public HdfsService getHbaseService() {
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        conf.set("fs.defaultFS", defaultHdfsUri);
        return new HdfsService(conf, defaultHdfsUri);
    }
}
