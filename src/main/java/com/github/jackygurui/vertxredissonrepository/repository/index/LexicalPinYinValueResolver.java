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
import com.github.pffy.chinese.HanyuPinyin;
import io.vertx.core.json.JsonObject;

/**
 *
 * @author Rui Gu
 */
public class LexicalPinYinValueResolver implements RedisIndexValueResolver<String> {

    static {
        //warm up;
        HanyuPinyin.convert("");
    }

    @Override
    public String resolve(String value, JsonObject root, String id, String fieldName, RedissonIndex index) {
        return HanyuPinyin.convert(value) + RedisRepository.DEFAULT_SEPERATOR + id;
    }

    @Override
    public String lookup(String indexedValue, RedissonIndex index) {
        int i = indexedValue.lastIndexOf(RedisRepository.DEFAULT_SEPERATOR);
        return (i == -1 || i == indexedValue.length() - 1) ? indexedValue : indexedValue.substring(i + 1, indexedValue.length() - 1);
    }

}
