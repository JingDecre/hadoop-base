package com.decre.cache.redis.service;

import com.decre.common.entity.User;

/**
 * @author Decre
 * @date 2019-5-29 23:50
 * @since 1.0.0
 * Descirption:
 */
public interface IUserService {

    /**
     * 删除
     *
     * @param user 用户对象
     * @return 操作结果
     */
    User saveOrUpdate(User user);

    /**
     * 添加
     *
     * @param id key值
     * @return 返回结果
     */
    User get(Long id);

    /**
     * 删除
     *
     * @param id key值
     */
    void delete(Long id);

}
