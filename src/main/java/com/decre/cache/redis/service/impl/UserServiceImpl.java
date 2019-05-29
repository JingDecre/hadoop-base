package com.decre.cache.redis.service.impl;

import com.decre.cache.redis.service.IUserService;
import com.decre.common.entity.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Decre
 * @date 2019-5-29 23:51
 * @since 1.0.0
 * Descirption:
 */
@Service
public class UserServiceImpl implements IUserService {

    private static final Map<Long, User> DATABASES = new HashMap<>();

    static {
        DATABASES.put(1L, new User(1L, "u1", "p1"));
        DATABASES.put(2L, new User(2L, "u2", "p2"));
        DATABASES.put(3L, new User(3L, "u3", "p3"));
    }

    private static final Logger log = LoggerFactory.getLogger(UserServiceImpl.class);

    @Cacheable(value = "user", key = "#user.id")
    @Override
    public User saveOrUpdate(User user) {
        DATABASES.put(user.getId(), user);
        log.info("进入 saveOrUpdate 方法");
        return user;

    }

    @CachePut(value = "user", key = "#id")
    @Override
    public User get(Long id) {
        log.info("进入 get 方法");
        return DATABASES.get(id);

    }

    @CacheEvict(value = "user", key = "#id")
    @Override
    public void delete(Long id) {
        DATABASES.remove(id);
        log.info("进入 delete 方法");

    }
}
