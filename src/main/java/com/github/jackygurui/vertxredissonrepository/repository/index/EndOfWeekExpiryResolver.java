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

import com.github.jackygurui.vertxredissonrepository.annotation.RedissonCounter;
import io.vertx.core.json.JsonObject;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.temporal.WeekFields;
import java.util.Locale;

/**
 *
 * @author Rui Gu
 */
public class EndOfWeekExpiryResolver implements RedisRelativeTimeExpiryResolver<Object> {

    @Override
    public long resolve(Object value, JsonObject root, String id, String fieldName, RedissonCounter counter) {
        return LocalTime.of(23, 59, 59, 999999999).atDate(LocalDate.now().with(WeekFields.of(Locale.CHINA).dayOfWeek(), 7)).atZone(ZoneId.systemDefault()).toEpochSecond() * 1000 + 999;
    }

}
