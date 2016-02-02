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
package com.github.jackygurui.vertxredissonrepository.repository;

import io.vertx.core.AsyncResult;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import java.util.List;

/**
 *
 * @author Rui Gu
 * @param <T>
 */
public interface RedisRepository<T> {

    static final String DEFAULT_SEPERATOR = "#";
    static final String DEFAULT_ID_SUFFIX = "ID";
    static final String DEFAULT_SEQUENCE_SUFFIX = "SEQ";
    static final String DEFAULT_UNIQUE_SUFFIX = "UNQ";
    static final String DEFAULT_COUNTER_SUFFIX = "CNTR";
    static final String DEFAULT_INDEX_SUFFIX = "IDX";
    static final String DEFAULT_INDEX_NAME = "DEFAULT";

    public void get(String id, AsyncResultHandler<T> resultHandler);

    public void getAll(AsyncResultHandler<List<T>> resultHandler);

    public void getAllIds(AsyncResultHandler<List<String>> resultHandler);

    public void getByList(List<String> ids, AsyncResultHandler<List<T>> resultHandler);

    public void searchUniqueIndex(String fieldName, String value, AsyncResultHandler<String> resultHandler);

    public void searchIndexByPosition(String fieldName, Integer start, Integer stop, AsyncResultHandler<List<String>> resultHandler);

    public void searchIndexByPosition(String fieldName, String indexName, Integer start, Integer stop, AsyncResultHandler<List<String>> resultHandler);

    public void searchIndexByScore(String fieldName, Double min, Double max, Integer offset, Integer limit, AsyncResultHandler<List<String>> resultHandler);

    public void searchIndexByScore(String fieldName, String indexName, Double min, Double max, Integer offset, Integer limit, AsyncResultHandler<List<String>> resultHandler);

    public void searchIndexByValue(String fieldName, String min, String max, Integer offset, Integer limit, AsyncResultHandler<List<String>> resultHandler);

    public void searchIndexByValue(String fieldName, String indexName, String min, String max, Integer offset, Integer limit, AsyncResultHandler<List<String>> resultHandler);

    public void searchUniqueIndexAndGet(String fieldName, String value, AsyncResultHandler<T> resultHandler);

    public void searchIndexByPositionAndGet(String fieldName, Integer start, Integer stop, AsyncResultHandler<List<T>> resultHandler);

    public void searchIndexByPositionAndGet(String fieldName, String indexName, Integer start, Integer stop, AsyncResultHandler<List<T>> resultHandler);

    public void searchIndexByScoreAndGet(String fieldName, Double min, Double max, Integer offset, Integer limit, AsyncResultHandler<List<T>> resultHandler);

    public void searchIndexByScoreAndGet(String fieldName, String indexName, Double min, Double max, Integer offset, Integer limit, AsyncResultHandler<List<T>> resultHandler);

    public void searchIndexByValueAndGet(String fieldName, String min, String max, Integer offset, Integer limit, AsyncResultHandler<List<T>> resultHandler);

    public void searchIndexByValueAndGet(String fieldName, String indexName, String min, String max, Integer offset, Integer limit, AsyncResultHandler<List<T>> resultHandler);

    public void indexValue(String id, String fieldName, String indexName, JsonObject instance, AsyncResultHandler<Boolean> resultHandler);

    public void unindexValue(String id, String fieldName, String indexName, JsonObject instance, AsyncResultHandler<Boolean> resultHandler);

    public void create(String data, AsyncResultHandler<String> resultHandler);

    public void update(String id, String data, Handler<AsyncResult<Boolean>> resultHandler);

    public void delete(String id, Handler<AsyncResult<Boolean>> resultHandler);

    public void totalCount(Handler<AsyncResult<Long>> resultHandler);
}
