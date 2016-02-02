/*
 * Copyright 2016 Rui Gu (jackygurui@gmail.com).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jackygurui.vertxredissonrepository.repository.Impl;

import com.github.jackygurui.vertxredissonrepository.provider.RedissonProvider;
import com.github.jackygurui.vertxredissonrepository.repository.RedisRepository;
import com.github.jackygurui.vertxredissonrepository.repository.RedisRepositoryFactory;
import com.github.jackygurui.vertxredissonrepository.repository.RepositoryException;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.redisson.Redisson;

/**
 *
 * @author Rui Gu
 */
public class RedisRepositoryFactoryImpl implements RedisRepositoryFactory {

    private final Vertx vertx;
    private final Redisson redissonRead;
    private final Redisson redissonWrite;
    private final Redisson redissonOther;
    private final HashMap<Class, RedisRepository> cache = new HashMap<>();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public RedisRepositoryFactoryImpl(Vertx vertx, JsonObject config) {
        this.vertx = vertx;
        this.redissonRead = RedissonProvider.create(config);
        this.redissonWrite = RedissonProvider.create(config);
        this.redissonOther = RedissonProvider.create(config);
    }

    @Override
    public RedisRepository instance(Class cls) throws RepositoryException {
        if (shutdown.get()) {
            throw new RepositoryException("Shutting down.");
        }
        if (!cache.containsKey(cls)) {
            cache.put(cls, new RedisRepositoryImpl(vertx, redissonRead, redissonWrite, redissonOther, cls, this));
        }
        return cache.get(cls);
    }

    @Override
    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            this.redissonRead.shutdown();
            this.redissonWrite.shutdown();
            this.redissonOther.shutdown();
        }
    }
}
