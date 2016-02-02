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
package com.github.jackygurui.vertxredissonrepository.provider;

import io.vertx.core.json.JsonObject;
import org.redisson.Config;
import org.redisson.Redisson;

/**
 *
 * @author Rui Gu
 */
public interface RedissonProvider {

    public static Redisson create(JsonObject jo) {
        Config redisConfig = new Config();
        redisConfig.useSingleServer().setAddress(jo.getString("serverAddress", "localhost:6379"));
        return (Redisson) Redisson.create(redisConfig);
    }

    public static Redisson create() {
        return create(new JsonObject());
    }
}
