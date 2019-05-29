package com.decre.hadoop.hadoopbase;

import com.decre.cache.redis.service.IUserService;
import com.decre.common.entity.User;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RedisCacheTests {

    @Autowired
    private IUserService userService;

    private static final Logger log = LoggerFactory.getLogger(RedisCacheTests.class);

    /**
     * 测试创建HDFS目录
     */
    @Test
    public void testRedisCache() {
        final User user = userService.saveOrUpdate(new User(5L, "u5", "p5"));
        log.info("[saveOrUpdate] - [{}]", user);
        final User user1 = userService.get(5L);
        log.info("[get] - [{}]", user1);
        userService.delete(5L);

    }

    /**
     * 测试上传文件
     */
    @Test
    public void testUploadFile() {

    }


}
