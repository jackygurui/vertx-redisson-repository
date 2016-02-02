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
package com.github.jackygurui.vertxredissonrepository.repository.index;

import com.github.jackygurui.vertxredissonrepository.annotation.RedissonIndex;
import com.github.jackygurui.vertxredissonrepository.repository.RedisRepository;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;

/**
 *
 * @author Rui Gu
 */
public class DefaultCompoundValueResolver implements RedisIndexValueResolver<Object> {

    @Override
    public String resolve(Object value, JsonObject root, String id, String fieldName, RedissonIndex index) {
        AtomicReference<String> s = new AtomicReference<>();
        if (StringUtils.isBlank(index.valueField())) {
            s.set(value instanceof String ? (String) value : Json.encode(value));
        } else {
            s.set(root.getString(index.valueField()));
        }
        Arrays.stream(index.compoundIndexFields()).forEach(e -> s.set(s.get().concat(RedisRepository.DEFAULT_SEPERATOR).concat(root.getValue(e).toString())));
        return s.get().concat(RedisRepository.DEFAULT_SEPERATOR).concat(id);
    }

    @Override
    public String lookup(String indexedValue, RedissonIndex index) {
        int i = indexedValue.lastIndexOf(RedisRepository.DEFAULT_SEPERATOR);
        return (i == -1 || i == indexedValue.length() - 1) ? indexedValue : indexedValue.substring(i + 1, indexedValue.length());
    }

}
