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
package com.github.jackygurui.vertxredissonrepository.repository.type;

import com.github.jackygurui.vertxredissonrepository.annotation.RedissonCounter;
import com.github.jackygurui.vertxredissonrepository.annotation.RedissonEntity;
import com.github.jackygurui.vertxredissonrepository.annotation.RedissonIndex;
import com.github.jackygurui.vertxredissonrepository.repository.index.DefaultCompoundValueResolver;
import com.github.jackygurui.vertxredissonrepository.repository.index.EndOfDayExpiryResolver;
import com.github.jackygurui.vertxredissonrepository.repository.index.EndOfMonthExpiryResolver;
import com.github.jackygurui.vertxredissonrepository.repository.index.EndOfWeekExpiryResolver;
import com.github.jackygurui.vertxredissonrepository.repository.index.EndOfYearExpiryResolver;
import com.github.jackygurui.vertxredissonrepository.repository.index.UnixTimestampScoreResolver;
import java.util.Arrays;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 *
 * @author Rui Gu
 */
@Data
@RedissonEntity
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CallIn {

    public enum Status {

        RE("RESOLVED"),//resolved
        IP("IN_PROGRESS"),//In progress
        UR("UNRESOLVED");//Unresolve i.e. new

        private final String indexName;

        private Status(String indexName) {
            this.indexName = indexName;
        }

        public String getIndexName() {
            return indexName;
        }
    }

    public enum Type {

        ENQ("ENQUIRY"),//Enquiry
        REQ("REQUEST");//Request

        private final String typeName;

        private Type(String typeName) {
            this.typeName = typeName;
        }

        public String getTypeName() {
            return typeName;
        }

        public static Type getType(String name) {
            return Arrays.stream(Type.values()).filter(e -> e.getTypeName().equals(name)).findFirst().get();
        }
    }

    @RedissonCounter.List({
        @RedissonCounter(name = "TODAY", expiryResolver = EndOfDayExpiryResolver.class),
        @RedissonCounter(name = "WEEK", expiryResolver = EndOfWeekExpiryResolver.class),
        @RedissonCounter(name = "MONTH", expiryResolver = EndOfMonthExpiryResolver.class),
        @RedissonCounter(name = "YEAR", expiryResolver = EndOfYearExpiryResolver.class)
    })
    private String id;
    private Integer type;
    private Integer gender;
    private String customerId;
    private String fullName;
    private String birthDay;
    private String phoneNumber;
    private Type callType;
    @RedissonIndex.List({
        @RedissonIndex(name = "DEFAULT",
                scoreField = "callTime",
                compoundIndexFields = {"callTime", "callType"},
                scoreResolver = UnixTimestampScoreResolver.class,
                valueResolver = DefaultCompoundValueResolver.class),
        @RedissonIndex(name = "UNRESOLVED",
                scoreField = "callTime",
                compoundIndexFields = {"callTime", "callType"},
                scoreResolver = UnixTimestampScoreResolver.class,
                valueResolver = DefaultCompoundValueResolver.class),
        @RedissonIndex(name = "IN_PROGRESS",
                indexOnSave = false,
                scoreField = "callTime",
                compoundIndexFields = {"callTime", "callType"},
                scoreResolver = UnixTimestampScoreResolver.class,
                valueResolver = DefaultCompoundValueResolver.class),
        @RedissonIndex(name = "RESOLVED",
                indexOnSave = false,
                scoreField = "callTime",
                compoundIndexFields = {"callTime", "callType"},
                scoreResolver = UnixTimestampScoreResolver.class,
                valueResolver = DefaultCompoundValueResolver.class)
    })
    private String province;
    @RedissonIndex.List({
        @RedissonIndex(name = "DEFAULT",
                scoreField = "callTime",
                compoundIndexFields = {"callTime", "callType"},
                scoreResolver = UnixTimestampScoreResolver.class,
                valueResolver = DefaultCompoundValueResolver.class),
        @RedissonIndex(name = "UNRESOLVED",
                scoreField = "callTime",
                compoundIndexFields = {"callTime", "callType"},
                scoreResolver = UnixTimestampScoreResolver.class,
                valueResolver = DefaultCompoundValueResolver.class),
        @RedissonIndex(name = "IN_PROGRESS",
                indexOnSave = false,
                scoreField = "callTime",
                compoundIndexFields = {"callTime", "callType"},
                scoreResolver = UnixTimestampScoreResolver.class,
                valueResolver = DefaultCompoundValueResolver.class),
        @RedissonIndex(name = "RESOLVED",
                indexOnSave = false,
                scoreField = "callTime",
                compoundIndexFields = {"callTime", "callType"},
                scoreResolver = UnixTimestampScoreResolver.class,
                valueResolver = DefaultCompoundValueResolver.class)
    })
    private String city;
    @RedissonIndex.List({
        @RedissonIndex(name = "DEFAULT",
                scoreField = "callTime",
                compoundIndexFields = {"callTime", "callType"},
                scoreResolver = UnixTimestampScoreResolver.class,
                valueResolver = DefaultCompoundValueResolver.class),
        @RedissonIndex(name = "UNRESOLVED",
                scoreField = "callTime",
                compoundIndexFields = {"callTime", "callType"},
                scoreResolver = UnixTimestampScoreResolver.class,
                valueResolver = DefaultCompoundValueResolver.class),
        @RedissonIndex(name = "IN_PROGRESS",
                indexOnSave = false,
                scoreField = "callTime",
                compoundIndexFields = {"callTime", "callType"},
                scoreResolver = UnixTimestampScoreResolver.class,
                valueResolver = DefaultCompoundValueResolver.class),
        @RedissonIndex(name = "RESOLVED",
                indexOnSave = false,
                scoreField = "callTime",
                compoundIndexFields = {"callTime", "callType"},
                scoreResolver = UnixTimestampScoreResolver.class,
                valueResolver = DefaultCompoundValueResolver.class)
    })
    private String area;
    private String address;
    @RedissonIndex.List({
        @RedissonIndex(name = "DEFAULT",
                compoundIndexFields = {"callType"},
                scoreResolver = UnixTimestampScoreResolver.class,
                valueResolver = DefaultCompoundValueResolver.class),
        @RedissonIndex(name = "UNRESOLVED",
                compoundIndexFields = {"callType"},
                scoreResolver = UnixTimestampScoreResolver.class,
                valueResolver = DefaultCompoundValueResolver.class),
        @RedissonIndex(name = "IN_PROGRESS",
                indexOnSave = false,
                compoundIndexFields = {"callType"},
                scoreResolver = UnixTimestampScoreResolver.class,
                valueResolver = DefaultCompoundValueResolver.class),
        @RedissonIndex(name = "RESOLVED",
                indexOnSave = false,
                compoundIndexFields = {"callType"},
                scoreResolver = UnixTimestampScoreResolver.class,
                valueResolver = DefaultCompoundValueResolver.class)
    })
    private Long callTime;
    private Status status = Status.UR;
    private String comments;
}
