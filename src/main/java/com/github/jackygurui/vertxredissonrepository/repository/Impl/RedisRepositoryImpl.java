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

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.github.fge.jsonschema.main.JsonValidator;
import com.github.jackygurui.vertxredissonrepository.annotation.JsonSchemaValid;
import com.github.jackygurui.vertxredissonrepository.annotation.RedissonCounter;
import com.github.jackygurui.vertxredissonrepository.annotation.RedissonEntity;
import com.github.jackygurui.vertxredissonrepository.annotation.RedissonIndex;
import com.github.jackygurui.vertxredissonrepository.annotation.RedissonUnique;
import com.github.jackygurui.vertxredissonrepository.repository.RedisRepository;
import com.github.jackygurui.vertxredissonrepository.repository.RedisRepositoryFactory;
import com.github.jackygurui.vertxredissonrepository.repository.RepositoryException;
import com.github.jackygurui.vertxredissonrepository.repository.index.LexicalScoreResolver;
import com.github.jackygurui.vertxredissonrepository.repository.index.RedisPreciseTimeExpiryResolver;
import com.github.reinert.jjschema.v1.JsonSchemaV4Factory;
import io.netty.util.concurrent.GenericFutureListener;
import io.vertx.core.AsyncResult;
import io.vertx.core.AsyncResultHandler;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.beanutils.BeanMap;
import org.redisson.Redisson;
import org.redisson.client.codec.StringCodec;
import org.redisson.core.RBatch;
import org.simondean.vertx.async.Async;

/**
 *
 * @author Rui Gu
 * @param <T>
 */
public class RedisRepositoryImpl<T> implements RedisRepository<T> {

    private static final Logger logger = LoggerFactory.getLogger(RedisRepositoryImpl.class);
    private static final JsonValidator VALIDATOR = JsonSchemaFactory.byDefault().getValidator();
    private static final com.github.reinert.jjschema.v1.JsonSchemaFactory schemaFactory = new JsonSchemaV4Factory();

    static {
        schemaFactory.setAutoPutDollarSchema(true);
    }

    private final Vertx vertx;
    private final Redisson redissonRead;
    private final Redisson redissonWrite;
    private final Redisson redissonOther;
    private final JsonNode schema;
    private final Class<T> cls;
    private final RedisRepositoryFactory factory;

    public RedisRepositoryImpl(Vertx vertx, Redisson redissonRead, Redisson redissonWrite, Redisson redissonOther, Class<T> cls, RedisRepositoryFactory factory) {
        this.vertx = vertx;
        this.redissonRead = redissonRead;
        this.redissonWrite = redissonWrite;
        this.redissonOther = redissonOther;
        this.schema = cls.isAnnotationPresent(JsonSchemaValid.class) ? schemaFactory.createSchema(cls) : null;
        this.cls = cls;
        this.factory = factory;
    }

    @Override
    public void get(String id, AsyncResultHandler<T> resultHandler) {
        vertx.<T>executeBlocking(f -> {
            getBlocking(id, r -> {
                if (r.succeeded() && !f.isComplete()) {
                    f.complete(r.result());
                } else if (!f.isComplete()) {
                    f.fail(r.cause());
                }
            });
        }, resultHandler);
    }

    private void getBlocking(String id, AsyncResultHandler<T> resultHandler) {
        idExist(id, r -> {
            if (r.succeeded() && r.result()) {
                deepFetch(id, resultHandler);
            } else {
                resultHandler.handle(Future.failedFuture(r.cause()));
            }
        });
    }

    @Override
    public void getAll(AsyncResultHandler<List<T>> resultHandler) {
        vertx.<List<T>>executeBlocking(f -> {
            getAllBlocking(r -> {
                if (r.succeeded() && !f.isComplete()) {
                    f.complete(r.result());
                } else if (!f.isComplete()) {
                    f.fail(r.cause());
                }
            });
        }, resultHandler);
    }

    private void getAllBlocking(AsyncResultHandler<List<T>> resultHandler) {
        Async.waterfall()
                .<List<String>>task(this::getAllIds)
                .<List<T>>task(this::getByListBlocking)
                .run(resultHandler);
    }

    @Override
    public void getAllIds(AsyncResultHandler<List<String>> resultHandler) {
        vertx.<List<String>>executeBlocking(f -> {
            getAllIdsBlocking(r -> {
                if (r.succeeded() && !f.isComplete()) {
                    f.complete(r.result());
                } else if (!f.isComplete()) {
                    f.fail(r.cause());
                }
            });
        }, resultHandler);
    }

    private void getAllIdsBlocking(AsyncResultHandler<List<String>> resultHandler) {
        redissonOther.<String, String>getMap(getStorageKey(), StringCodec.INSTANCE).keySetAsync().addListener(future -> {
            resultHandler.handle(future.isSuccess() ? (future.get() != null ? Future.succeededFuture(((Set<String>) future.get()).stream().collect(Collectors.toList())) : Future.failedFuture(new RepositoryException("No search result returned"))) : Future.failedFuture(new RepositoryException(future.cause())));
            if (future.isSuccess()) {
                logger.debug("[" + getStorageKey() + "] Get ALL Keys in Unique [" + getUniqueKey("id") + "] Success.");
            } else {
                logger.debug("[" + getStorageKey() + "] Get ALL Keys in Unique [" + getUniqueKey("id") + "] Failed: ", future.cause());
            }
        });
    }

    @Override
    public void getByList(List<String> ids, AsyncResultHandler<List<T>> resultHandler) {
        vertx.<List<T>>executeBlocking(f -> {
            getByListBlocking(ids, r -> {
                if (r.succeeded() && !f.isComplete()) {
                    f.complete(r.result());
                } else if (!f.isComplete()) {
                    f.fail(r.cause());
                }
            });
        }, resultHandler);
    }

    private void getByListBlocking(List<String> ids, AsyncResultHandler<List<T>> resultHandler) {
        if (ids == null) {
            resultHandler.handle(Future.failedFuture(new IllegalArgumentException("List of ids can't be null.")));
            return;
        } else if (ids.isEmpty()) {
            resultHandler.handle(Future.succeededFuture(Collections.emptyList()));
            return;
        }
        AtomicLong c = new AtomicLong(0);
        ArrayList<T> l = new ArrayList<>(ids.size());
        IntStream.range(0, ids.size()).forEach(i -> l.add(null));
        ids.stream().forEach(e -> {
            getBlocking(e, r -> {
                l.set(ids.indexOf(e), r.result());
                if (c.incrementAndGet() == ids.size()) {
                    resultHandler.handle(Future.succeededFuture(l.stream().filter(s -> s != null).collect(Collectors.toCollection(ArrayList::new))));
                }
            });
        });
    }

    @Override
    public void searchUniqueIndex(String fieldName, String value, AsyncResultHandler<String> resultHandler) {
        vertx.<String>executeBlocking(f -> {
            getIdByUnique(fieldName, value, r -> {
                if (r.succeeded() && !f.isComplete()) {
                    f.complete(r.result());
                } else if (!f.isComplete()) {
                    f.fail(r.cause());
                }
            });
        }, resultHandler);
    }

    @Override
    public void searchUniqueIndexAndGet(String fieldName, String value, AsyncResultHandler<T> resultHandler) {
        vertx.<T>executeBlocking(f -> {
            getByUniqueBlocking(fieldName, value, r -> {
                if (r.succeeded() && !f.isComplete()) {
                    f.complete(r.result());
                } else if (!f.isComplete()) {
                    f.fail(r.cause());
                }
            });
        }, resultHandler);
    }

    private void getByUniqueBlocking(String fieldName, String value, AsyncResultHandler<T> resultHandler) {
        Async.waterfall()
                .<String>task(t -> getIdByUnique(fieldName, value, t))
                .<T>task(this::deepFetch)
                .<T>run(resultHandler);
    }

    @Override
    public void searchIndexByPosition(String fieldName, Integer start, Integer stop, AsyncResultHandler<List<String>> resultHandler) {
        searchIndexByPosition(fieldName, "DEFAULT", start, stop, resultHandler);
    }

    @Override
    public void searchIndexByPosition(String fieldName, String indexName, Integer start, Integer stop, AsyncResultHandler<List<String>> resultHandler) {
        vertx.<List<String>>executeBlocking(f -> {
            searchIndexByPositionBlocking(fieldName, indexName, start, stop, r -> {
                if (r.succeeded() && !f.isComplete()) {
                    f.complete(r.result());
                } else if (!f.isComplete()) {
                    f.fail(r.cause());
                }
            });
        }, resultHandler);
    }

    private void searchIndexByPositionBlocking(String fieldName, String indexName, Integer start, Integer stop, AsyncResultHandler<List<String>> resultHandler) {
        redissonOther.getScoredSortedSet(getScoredSortedIndexKey(fieldName, indexName), StringCodec.INSTANCE).valueRangeAsync(start, stop).addListener(future -> {
            Collection<String> res = (Collection<String>) future.get();
            try {
                ArrayList<String> ids = res.stream().map(r -> lookupIdFromIndex(r, fieldName, indexName)).collect(Collectors.toCollection(ArrayList::new));
                resultHandler.handle(future.isSuccess() ? (future.get() != null ? Future.succeededFuture(ids) : Future.failedFuture(new RepositoryException("No search result returned"))) : Future.failedFuture(new RepositoryException(future.cause())));
                if (future.isSuccess()) {
                    logger.debug("[" + getStorageKey() + "] Search Index By Position Success. Records: " + ((Collection<String>) future.get()).size());
                } else {
                    logger.debug("[" + getStorageKey() + "] Search Index By Position Failure.", future.cause());
                }
            } catch (RuntimeException e) {
                resultHandler.handle(Future.failedFuture(e.getCause()));
                logger.debug("[" + getStorageKey() + "] Search Index By Position Failure.", e);
            }
        });
    }

    @Override
    public void searchIndexByScore(String fieldName, Double min, Double max, Integer offset, Integer limit, AsyncResultHandler<List<String>> resultHandler) {
        searchIndexByScore(fieldName, "DEFAULT", min, max, offset, limit, resultHandler);
    }

    @Override
    public void searchIndexByScore(String fieldName, String indexName, Double min, Double max, Integer offset, Integer limit, AsyncResultHandler<List<String>> resultHandler) {
        vertx.<List<String>>executeBlocking(f -> {
            searchIndexByScoreBlocking(fieldName, indexName, min, max, offset, limit, r -> {
                if (r.succeeded() && !f.isComplete()) {
                    f.complete(r.result());
                } else if (!f.isComplete()) {
                    f.fail(r.cause());
                }
            });
        }, resultHandler);
    }

    private void searchIndexByScoreBlocking(String fieldName, String indexName, Double min, Double max, Integer offset, Integer limit, AsyncResultHandler<List<String>> resultHandler) {
        redissonOther.<String>getScoredSortedSet(getScoredSortedIndexKey(fieldName, indexName), StringCodec.INSTANCE).valueRangeAsync(min, true, max, true, offset, limit).addListener(future -> {
            Collection<String> res = (Collection<String>) future.get();
            try {
                ArrayList<String> ids = res.stream().map(r -> lookupIdFromIndex(r, fieldName, indexName)).collect(Collectors.toCollection(ArrayList::new));
                resultHandler.handle(future.isSuccess() ? (future.get() != null || ids == null ? Future.succeededFuture(ids) : Future.failedFuture(new RepositoryException("No search result returned"))) : Future.failedFuture(new RepositoryException(future.cause())));
                if (future.isSuccess()) {
                    logger.debug("[" + getStorageKey() + "] Search Index By Score Success. Records: " + ((Collection<String>) future.get()).size());
                } else {
                    logger.debug("[" + getStorageKey() + "] Search Index By Score Failure.", future.cause());
                }
            } catch (RuntimeException e) {
                resultHandler.handle(Future.failedFuture(e.getCause()));
                logger.debug("[" + getStorageKey() + "] Search Index By Score Failure.", e);
            }
        });
    }

    @Override
    public void searchIndexByValue(String fieldName, String min, String max, Integer offset, Integer limit, AsyncResultHandler<List<String>> resultHandler) {
        searchIndexByValue(fieldName, "DEFAULT", min, max, offset, limit, resultHandler);
    }

    @Override
    public void searchIndexByValue(String fieldName, String indexName, String min, String max, Integer offset, Integer limit, AsyncResultHandler<List<String>> resultHandler) {
        vertx.<List<String>>executeBlocking(f -> {
            searchIndexByValueBlocking(fieldName, indexName, min, max, offset, limit, r -> {
                if (r.succeeded() && !f.isComplete()) {
                    f.complete(r.result());
                } else if (!f.isComplete()) {
                    f.fail(r.cause());
                }
            });
        }, resultHandler);
    }

    private void searchIndexByValueBlocking(String fieldName, String indexName, String min, String max, Integer offset, Integer limit, AsyncResultHandler<List<String>> resultHandler) {
        redissonOther.<String>getLexSortedSet(getScoredSortedIndexKey(fieldName, indexName)).lexRangeAsync(min, true, max, true, offset, limit).addListener(future -> {
            Collection<String> res = (Collection<String>) future.get();
            try {
                ArrayList<String> ids = res.stream().map(r -> lookupIdFromIndex(r, fieldName, indexName)).collect(Collectors.toCollection(ArrayList::new));
                resultHandler.handle(future.isSuccess() ? (future.get() != null ? Future.succeededFuture(ids) : Future.failedFuture(new RepositoryException("No search result returned"))) : Future.failedFuture(new RepositoryException(future.cause())));
                if (future.isSuccess()) {
                    logger.debug("[" + getStorageKey() + "] Search Index By Value Success. Records: " + ((Collection<String>) future.get()).size());
                } else {
                    logger.debug("[" + getStorageKey() + "] Search Index By Value Failure.", future.cause());
                }
            } catch (RuntimeException e) {
                resultHandler.handle(Future.failedFuture(e.getCause()));
                logger.debug("[" + getStorageKey() + "] Search Index By Value Failure.", e);
            }
        });
    }

    @Override
    public void searchIndexByPositionAndGet(String fieldName, Integer start, Integer stop, AsyncResultHandler<List<T>> resultHandler) {
        searchIndexByPositionAndGet(fieldName, "DEFAULT", start, stop, resultHandler);
    }

    @Override
    public void searchIndexByPositionAndGet(String fieldName, String indexName, Integer start, Integer stop, AsyncResultHandler<List<T>> resultHandler) {
        vertx.<List<T>>executeBlocking(f -> {
            searchIndexByPositionAndGetBlocking(fieldName, indexName, start, stop, r -> {
                if (r.succeeded() && !f.isComplete()) {
                    f.complete(r.result());
                } else if (!f.isComplete()) {
                    f.fail(r.cause());
                }
            });
        }, resultHandler);
    }

    private void searchIndexByPositionAndGetBlocking(String fieldName, String indexName, Integer start, Integer stop, AsyncResultHandler<List<T>> resultHandler) {
        Async.waterfall()
                .<List<String>>task(t -> searchIndexByPositionBlocking(fieldName, indexName, start, stop, t))
                .<List<T>>task(this::getByListBlocking)
                .run(resultHandler);
    }

    @Override
    public void searchIndexByScoreAndGet(String fieldName, Double min, Double max, Integer offset, Integer limit, AsyncResultHandler<List<T>> resultHandler) {
        searchIndexByScoreAndGet(fieldName, "DEFAULT", min, max, offset, limit, resultHandler);
    }

    @Override
    public void searchIndexByScoreAndGet(String fieldName, String indexName, Double min, Double max, Integer offset, Integer limit, AsyncResultHandler<List<T>> resultHandler) {
        vertx.<List<T>>executeBlocking(f -> {
            searchIndexByScoreAndGetBlocking(fieldName, indexName, min, max, offset, limit, r -> {
                if (r.succeeded() && !f.isComplete()) {
                    f.complete(r.result());
                } else if (!f.isComplete()) {
                    f.fail(r.cause());
                }
            });
        }, resultHandler);
    }

    private void searchIndexByScoreAndGetBlocking(String fieldName, String indexName, Double min, Double max, Integer offset, Integer limit, AsyncResultHandler<List<T>> resultHandler) {
        Async.waterfall()
                .<List<String>>task(t -> searchIndexByScoreBlocking(fieldName, indexName, min, max, offset, limit, t))
                .<List<T>>task(this::getByListBlocking)
                .run(resultHandler);
    }

    @Override
    public void searchIndexByValueAndGet(String fieldName, String min, String max, Integer offset, Integer limit, AsyncResultHandler<List<T>> resultHandler) {
        searchIndexByValueAndGet(fieldName, "DEFAULT", min, max, offset, limit, resultHandler);
    }

    @Override
    public void searchIndexByValueAndGet(String fieldName, String indexName, String min, String max, Integer offset, Integer limit, AsyncResultHandler<List<T>> resultHandler) {
        vertx.<List<T>>executeBlocking(f -> {
            searchIndexByValueAndGetBlocking(fieldName, indexName, min, max, offset, limit, r -> {
                if (r.succeeded() && !f.isComplete()) {
                    f.complete(r.result());
                } else if (!f.isComplete()) {
                    f.fail(r.cause());
                }
            });
        }, resultHandler);
    }

    private void searchIndexByValueAndGetBlocking(String fieldName, String indexName, String min, String max, Integer offset, Integer limit, AsyncResultHandler<List<T>> resultHandler) {
        Async.waterfall()
                .<List<String>>task(t -> searchIndexByValueBlocking(fieldName, indexName, min, max, offset, limit, t))
                .<List<T>>task(this::getByListBlocking)
                .run(resultHandler);
    }

    @Override
    public void create(String data, AsyncResultHandler<String> resultHandler) {
        create(data, null, resultHandler);
    }

    private void create(String data, RBatch redissonBatch, AsyncResultHandler<String> resultHandler) {
        vertx.<JsonObject>executeBlocking(f -> {
            JsonObject d = new JsonObject(data);
            if (schema == null && !f.isComplete()) {
                f.complete(d);
                return;
            }
            try {
                ProcessingReport validate = VALIDATOR.validate(schema, Json.mapper.readTree(data));
                if (validate.isSuccess() && !d.containsKey("id") && !f.isComplete()) {
                    f.complete(d);
                } else if (!f.isComplete()) {
                    f.fail(new RepositoryException("Invalid Data"));
                }
            } catch (ProcessingException | IOException ex) {
                if (!f.isComplete()) {
                    f.fail(ex);
                }
            }
        }, result -> {
            if (result.succeeded()) {
                createWithoutValidate(result.result(), redissonBatch, resultHandler);
            } else {
                resultHandler.handle(Future.failedFuture(result.cause()));
            }
        });
    }

    private void createWithoutValidate(JsonObject data, RBatch redissonBatch, AsyncResultHandler<String> resultHandler) {
        AtomicReference<String> id = new AtomicReference<>();
        Async.waterfall()
                .<String>task(this::newId)
                .<String>task((i, t) -> {
                    id.set(i);
                    ensureUniqueAndIndexing(id.get(), data, true, rs -> {
                        if (rs.succeeded() && rs.result() == null) {
                            t.handle(Future.succeededFuture());
                        } else if (rs.result() != null) {
                            t.handle(Future.failedFuture(new RepositoryException(rs.result())));
                        } else {
                            t.handle(Future.failedFuture(rs.cause()));
                        }
                    });
                }).<Boolean>task((rs, t) -> {
                    persist(id.get(), data.put("id", id.get()), redissonBatch, t);
                }).run(run -> {
                    resultHandler.handle(run.succeeded() ? Future.succeededFuture(id.get()) : Future.failedFuture(run.cause()));
                });
    }

    @Override
    public void update(String id, String data, Handler<AsyncResult<Boolean>> resultHandler) {
        update(id, data, null, resultHandler);
    }

    private void update(String id, String data, RBatch redissonBatch, Handler<AsyncResult<Boolean>> resultHandler) {
        vertx.<JsonObject>executeBlocking(f -> {
            JsonObject d = new JsonObject(data);
            if (schema == null && !f.isComplete()) {
                f.complete(d);
                return;
            }
            try {
                ProcessingReport validate = VALIDATOR.validate(schema, Json.mapper.readTree(data));
                if (id != null && validate.isSuccess() && d.containsKey("id") && id.equals(d.getString("id")) && !f.isComplete()) {
                    f.complete(d);
                } else if (!f.isComplete()) {
                    f.fail(new RepositoryException("Invalid Data"));
                }
            } catch (ProcessingException | IOException ex) {
                if (!f.isComplete()) {
                    f.fail(ex);
                }
            }
        }, result -> {
            if (result.succeeded()) {
                updateWithoutValidate(id, result.result(), redissonBatch, resultHandler);
            } else {
                resultHandler.handle(Future.failedFuture(result.cause()));
            }
        });
    }

    private void updateWithoutValidate(String id, JsonObject data, RBatch redissonBatch, Handler<AsyncResult<Boolean>> resultHandler) {
        vertx.<Boolean>executeBlocking(f -> {
            Async.waterfall()
                    .<Boolean>task(t -> idExist(id, t))
                    .<String>task((idExist, t) -> {
                        if (!idExist) {
                            t.handle(Future.failedFuture(new RepositoryException("ID: " + id + " does not exist")));
                            return;
                        }
                        ensureUniqueAndIndexing(id, data, false, rs -> {
                            if (rs.succeeded() && rs.result() == null) {
                                t.handle(Future.succeededFuture());
                            } else if (rs.result() != null) {
                                t.handle(Future.failedFuture(rs.result()));
                            } else {
                                t.handle(Future.failedFuture(rs.cause()));
                            }
                        });

                    }).<Boolean>task((rs, t) -> persist(id, data, redissonBatch, t))
                    .run(run -> {
                        if (run.succeeded() && !f.isComplete()) {
                            f.complete(run.result());
                        } else if (!f.isComplete()) {
                            f.fail(run.cause());
                        }
                    });
        }, resultHandler);
    }

    @Override
    public void delete(String id, Handler<AsyncResult<Boolean>> resultHandler) {
        vertx.executeBlocking(f -> {
            deleteBlocking(id, r -> {
                if (r.succeeded() && !f.isComplete()) {
                    f.complete(r.result());
                } else if (!f.isComplete()) {
                    f.fail(r.cause());
                }
            });
        }, resultHandler);
    }

    private void deleteBlocking(String id, Handler<AsyncResult<Boolean>> resultHandler) {
        RBatch batch = redissonWrite.createBatch();
        //remove the indexes.
        batch.<String, String>getMap(getStorageKey(), StringCodec.INSTANCE).fastRemoveAsync(id);
        batch.getAtomicLongAsync(getCounterKey()).decrementAndGetAsync();
        batch.executeAsync().addListener(future -> {
            resultHandler.handle(future.isSuccess() ? (future.get() != null ? Future.succeededFuture(((Integer) ((List) future.get()).get(0)) == 1) : Future.failedFuture(new RepositoryException("No batch result returned"))) : Future.failedFuture(new RepositoryException(future.cause())));
            if (future.isSuccess()) {
                logger.debug("[" + getStorageKey() + "] Delete Success : " + id);
            } else {
                logger.debug("[" + getStorageKey() + "] Delete Failure : " + id, future.cause());
            }
        });
    }

    @Override
    public void totalCount(Handler<AsyncResult<Long>> resultHandler) {
        redissonOther.<String, String>getAtomicLong(getCounterKey()).getAsync().addListener(future -> {
            resultHandler.handle(future.isSuccess() ? (future.get() != null ? Future.succeededFuture((Long) future.get()) : Future.failedFuture(new RepositoryException("No search result returned"))) : Future.failedFuture(new RepositoryException(future.cause())));
            if (future.isSuccess()) {
                logger.debug("[" + getCounterKey() + "] Count Success : " + (Long) future.get());
            } else {
                logger.debug("[" + getCounterKey() + "] Count Failure ", future.cause());
            }
        });
    }

    private void idExist(String id, Handler<AsyncResult<Boolean>> resultHandler) {
        redissonOther.<String, String>getMap(getStorageKey(), StringCodec.INSTANCE).containsKeyAsync(id).addListener(future -> {
            resultHandler.handle(future.isSuccess() ? (future.get() != null ? Future.succeededFuture((Boolean) future.get()) : Future.failedFuture(new RepositoryException("No search result returned"))) : Future.failedFuture(new RepositoryException(future.cause())));
            if (future.isSuccess()) {
                logger.debug("[" + getStorageKey() + "] ID Exist Success : " + id);
            } else {
                logger.debug("[" + getStorageKey() + "] ID Exist Failure : " + id, future.cause());
            }
        });
    }

    private void getIdByUnique(String fieldName, String value, AsyncResultHandler<String> resultHandler) {
        redissonOther.<String, String>getMap(getUniqueKey(fieldName)).getAsync(value).addListener(future -> {
            resultHandler.handle(future.isSuccess() ? (future.get() != null ? Future.succeededFuture((String) future.get()) : Future.failedFuture(new RepositoryException("No search result returned"))) : Future.failedFuture(new RepositoryException(future.cause())));
            if (future.isSuccess()) {
                logger.debug("[" + getStorageKey() + "] Get ID By Unique Success : " + value + " - " + (String) future.get());
            } else {
                logger.debug("[" + getStorageKey() + "] Get ID By Unique : " + value, future.cause());
            }
        });
    }

    private void prepareEnsureUnique(String id, JsonObject instance, RBatch redissonBatch, ArrayList<String> pList, String parent) throws RepositoryException {
        try {
            BeanMap pMap = new BeanMap(cls.newInstance());
            pMap.keySet().forEach(e -> {
                if ("class".equals(e) || instance.getValue(e.toString()) == null) {
                    return;
                }
                Class type = pMap.getType(e.toString());
                String fieldName = (parent == null ? "" : parent.concat(".")).concat(e.toString());
                if (isRedisEntity(type)) {
                    try {
                        RedisRepositoryImpl innerRepo = (RedisRepositoryImpl) factory.instance(type);
                        innerRepo.prepareEnsureUnique(id, instance.getJsonObject(e.toString()), redissonBatch, pList, fieldName);
                    } catch (RepositoryException ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    try {
                        if (cls.getDeclaredField(e.toString()).isAnnotationPresent(RedissonUnique.class)) {
                            redissonBatch.getMap(getUniqueKey(e.toString())).putIfAbsentAsync(instance.getValue(e.toString()), id);
                            pList.add(fieldName);
                        }
                    } catch (NoSuchFieldException | SecurityException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });
        } catch (InstantiationException | IllegalAccessException | RuntimeException ex) {
            throw new RepositoryException(ex);
        }
    }

    private void prepareUndoUnique(String id, JsonObject instance, RBatch redissonBatch, ArrayList<String> pList, String parent) throws RepositoryException {
        try {
            BeanMap pMap = new BeanMap(cls.newInstance());
            pMap.keySet().forEach(e -> {
                if ("class".equals(e) || instance.getValue(e.toString()) == null) {
                    return;
                }
                Class type = pMap.getType(e.toString());
                String fieldName = (parent == null ? "" : parent.concat(".")).concat(e.toString());
                if (isRedisEntity(type)) {
                    try {
                        RedisRepositoryImpl innerRepo = (RedisRepositoryImpl) factory.instance(type);
                        innerRepo.prepareUndoUnique(id, instance.getJsonObject(e.toString()), redissonBatch, pList, fieldName);
                    } catch (RepositoryException ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    try {
                        if (cls.getDeclaredField(e.toString()).isAnnotationPresent(RedissonUnique.class) && pList.contains(fieldName)) {
                            redissonBatch.getMap(getUniqueKey(e.toString())).removeAsync(instance.getValue(e.toString()), id);
                        }
                    } catch (NoSuchFieldException | SecurityException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });
        } catch (InstantiationException | IllegalAccessException | RuntimeException ex) {
            throw new RepositoryException(ex);
        }
    }

    private void ensureUniqueAndIndexing(String id, JsonObject data, Boolean isNew, AsyncResultHandler<String> resultHandler) {
        vertx.<String>executeBlocking(f -> ensureUniqueAndIndexingBlocking(id, data, isNew, r -> {
            if (r.succeeded() && !f.isComplete()) {
                f.complete(r.result());
            } else if (!f.isComplete()) {
                f.fail(r.cause());
            }
        }), resultHandler);
    }

    private void ensureUniqueAndIndexingBlocking(String id, JsonObject data, Boolean isNew, AsyncResultHandler<String> resultHandler) {
        Async.waterfall().<String>task(t -> {
            RBatch batch = redissonOther.createBatch();
            ArrayList<String> pList = new ArrayList();
            try {
                prepareEnsureUnique(id, data, batch, pList, null);
            } catch (RepositoryException ex) {
                t.handle(Future.failedFuture(ex));
                return;
            }
            if (pList.isEmpty()) {
                t.handle(Future.succeededFuture());
                return;
            }
            batch.executeAsync().addListener(future -> {
                if (future.isSuccess() && future.get() != null) {
                    List<String> result = (List<String>) future.get();
                    Stream<String> filter = pList.stream().filter(p -> result.get(pList.indexOf(p)) != null && !result.get(pList.indexOf(p)).equals(id));//uniques are ensured by using putIfAbsent and all results should be null or itself to indicate no violation.
                    Object[] filterArray = filter.toArray();
                    if (filterArray.length != 0) {
                        t.handle(Future.succeededFuture(new JsonObject().put("uniqueViolation", Json.encode(filterArray)).encode()));
                        //now undo these things.
                        ArrayList<String> undoList = pList.stream().filter(p -> result.get(pList.indexOf(p)) == null || result.get(pList.indexOf(p)).equals(id)).collect(Collectors.toCollection(ArrayList::new));
                        Async.<Void>waterfall()
                                .<Void>task(task -> {
                                    RBatch b = redissonOther.createBatch();
                                    try {
                                        prepareUndoUnique(id, data, b, undoList, null);
                                    } catch (RepositoryException ex) {
                                        task.handle(Future.failedFuture(ex));
                                    }
                                    b.executeAsync().addListener(fu -> {
                                        task.handle(fu.isSuccess() ? Future.succeededFuture((Void) fu.get()) : Future.failedFuture(fu.cause()));
                                    });
                                }).run(run -> {
                                    logger.debug("Parallel undo indexing [id: " + id + "] " + (run.succeeded() ? "successed." : "failed."));
                                });
                    } else {
                        t.handle(Future.succeededFuture());
                    }
                } else {
                    t.handle(Future.failedFuture(!future.isSuccess() ? future.cause().fillInStackTrace() : new RepositoryException("No unique check result returned")));
                }
            });
        }).<String>task((violations, t) -> {
            if (violations != null) {
                t.handle(Future.failedFuture(violations));
                return;
            } else {
                t.handle(Future.succeededFuture());
            }
            //parallel indexing
            Async.<Void>waterfall()
                    .<T>task(task -> {
                        if (!isNew) {
                            fetch(id, Boolean.TRUE, task);
                        } else {
                            task.handle(Future.succeededFuture(null));
                        }
                    })
                    .<Void>task((r, task) -> {
                        reIndex(id, r, data, isNew, ri -> {
                            task.handle(ri.succeeded() ? Future.succeededFuture(ri.result()) : Future.failedFuture(ri.cause()));
                        });
                    }).run(run -> {
                        logger.debug("Parallel Indexing [id: " + id + "] " + (run.succeeded() ? "successed." : "failed."), run.cause());
                    });
        }).run(resultHandler);

    }

    private void reIndex(String id, T r, JsonObject data, Boolean isNew, Handler<AsyncResult<Void>> resultHandler) {
        vertx.<Void>executeBlocking(f -> reIndexBlocking(id, r, data, isNew, re -> {
            if (re.succeeded() && !f.isComplete()) {
                f.complete(re.result());
            } else if (!f.isComplete()) {
                f.fail(re.cause());
            }
        }), resultHandler);
    }

    private void prepareEnsuerIndex(String id, T r, JsonObject instance, Boolean isNew, RBatch redissonBatch, ArrayList<String> pList, String parent) throws RepositoryException {
        try {
            BeanMap pMap = new BeanMap(r == null ? cls.newInstance() : r);
            pMap.keySet().forEach(e -> {
                if ("class".equals(e) || instance.getValue(e.toString()) == null) {
                    return;
                }
                Class type = pMap.getType(e.toString());
                String fieldName = (parent == null ? "" : parent.concat(".")).concat(e.toString());
                if (isRedisEntity(type)) {
                    try {
                        RedisRepositoryImpl innerRepo = (RedisRepositoryImpl) factory.instance(type);
                        innerRepo.prepareEnsuerIndex(id, pMap.get(e), instance.getJsonObject(e.toString()), isNew, redissonBatch, pList, fieldName);
                    } catch (RepositoryException ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    try {
                        if (cls.getDeclaredField(e.toString()).isAnnotationPresent(RedissonIndex.class)) {
                            RedissonIndex index = cls.getDeclaredField(e.toString()).getAnnotation(RedissonIndex.class);
                            if (index.indexOnSave()) {
                                prepareEnsuerIndex(id, r, instance, redissonBatch, index, e.toString(), pMap.get(e));
                            }
                        } else if (cls.getDeclaredField(e.toString()).isAnnotationPresent(RedissonIndex.List.class)) {
                            RedissonIndex.List indexList = cls.getDeclaredField(e.toString()).getAnnotation(RedissonIndex.List.class);
                            Arrays.stream(indexList.value()).filter(f -> f.indexOnSave()).forEach(index -> prepareEnsuerIndex(id, r, instance, redissonBatch, index, e.toString(), pMap.get(e)));
                        }
                        if (cls.getDeclaredField(e.toString()).isAnnotationPresent(RedissonCounter.class)) {
                            RedissonCounter counter = cls.getDeclaredField(e.toString()).getAnnotation(RedissonCounter.class);
                            prepareEnsuerCounter(id, r, instance, isNew, redissonBatch, counter, e.toString(), pMap.get(e));
                        } else if (cls.getDeclaredField(e.toString()).isAnnotationPresent(RedissonCounter.List.class)) {
                            RedissonCounter.List counterList = cls.getDeclaredField(e.toString()).getAnnotation(RedissonCounter.List.class);
                            Arrays.stream(counterList.value()).forEach(index -> prepareEnsuerCounter(id, r, instance, isNew, redissonBatch, index, e.toString(), pMap.get(e)));
                        }
                    } catch (NoSuchFieldException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            });
        } catch (InstantiationException | IllegalAccessException | RuntimeException ex) {
            throw new RepositoryException(ex);
        }
    }

    private void prepareEnsuerIndex(String id, T r, JsonObject instance, RBatch redissonBatch, RedissonIndex index, String fieldName, Object fieldValue) throws RuntimeException {
        try {
            Object value = instance.getValue(fieldName);
            String resolve = index.valueResolver().newInstance().resolve(value, instance, id, fieldName, index);
            if (index.scoreResolver().isAssignableFrom(LexicalScoreResolver.class)) {
                redissonBatch.getLexSortedSet(getScoredSortedIndexKey(fieldName, index.name())).addAsync(resolve);
                if (r != null && !value.equals(fieldValue)) {
                    redissonBatch.getLexSortedSet(getScoredSortedIndexKey(fieldName, index.name())).removeAsync(index.valueResolver().newInstance().resolve(fieldValue, instance, id, fieldName, index));
                }
            } else {
                Double score = index.scoreResolver().newInstance().resolve(value, instance, id, fieldName, index);
                redissonBatch.getScoredSortedSet(getScoredSortedIndexKey(fieldName, index.name()), StringCodec.INSTANCE).addAsync(score, resolve);
                if (r != null && !value.equals(fieldValue)) {
                    redissonBatch.getScoredSortedSet(getScoredSortedIndexKey(fieldName, index.name()), StringCodec.INSTANCE).removeAsync(index.valueResolver().newInstance().resolve(fieldValue, instance, id, fieldName, index));
                }
            }
        } catch (InstantiationException | IllegalAccessException | RuntimeException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void indexValue(String id, String fieldName, String indexName, JsonObject instance, AsyncResultHandler<Boolean> resultHandler) {
        vertx.<Boolean>executeBlocking(f -> indexValueBlocking(id, fieldName, indexName, instance, re -> {
            if (re.succeeded() && !f.isComplete()) {
                f.complete(re.result());
            } else if (!f.isComplete()) {
                f.fail(re.cause());
            }
        }), resultHandler);
    }

    private void indexValueBlocking(String id, String fieldName, String indexName, JsonObject instance, AsyncResultHandler<Boolean> resultHandler) {
        try {
            if (cls.getDeclaredField(fieldName).isAnnotationPresent(RedissonIndex.class)) {
                RedissonIndex index = cls.getDeclaredField(fieldName).getAnnotation(RedissonIndex.class);
                indexValue(id, fieldName, index, instance, resultHandler);
            } else if (cls.getDeclaredField(fieldName).isAnnotationPresent(RedissonIndex.List.class)) {
                RedissonIndex.List indexList = cls.getDeclaredField(fieldName).getAnnotation(RedissonIndex.List.class);
                Arrays.stream(indexList.value()).filter(e -> e.name().equals(indexName)).findFirst().ifPresent(index -> indexValue(id, fieldName, index, instance, resultHandler));
            }
        } catch (NoSuchFieldException | RuntimeException e) {
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    private void indexValue(String id, String fieldName, RedissonIndex index, JsonObject instance, AsyncResultHandler<Boolean> resultHandler) {
        try {
            Object value = instance.getValue(fieldName);
            String resolve = index.valueResolver().newInstance().resolve(value, instance, id, fieldName, index);
            GenericFutureListener<io.netty.util.concurrent.Future<Boolean>> futureListener = (io.netty.util.concurrent.Future<Boolean> future) -> {
                resultHandler.handle(future.isSuccess() && future.get() != null ? Future.succeededFuture(future.get()) : Future.failedFuture(future.cause()));
            };
            if (index.scoreResolver().isAssignableFrom(LexicalScoreResolver.class)) {
                redissonOther.getLexSortedSet(getScoredSortedIndexKey(fieldName, index.name())).addAsync(resolve).addListener(futureListener);
            } else {
                Double score = index.scoreResolver().newInstance().resolve(value, instance, id, fieldName, index);
                redissonOther.getScoredSortedSet(getScoredSortedIndexKey(fieldName, index.name()), StringCodec.INSTANCE).addAsync(score, resolve).addListener(futureListener);
            }
        } catch (InstantiationException | IllegalAccessException | RuntimeException e) {
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    @Override
    public void unindexValue(String id, String fieldName, String indexName, JsonObject instance, AsyncResultHandler<Boolean> resultHandler) {
        vertx.<Boolean>executeBlocking(f -> unindexValueBlocking(id, fieldName, indexName, instance, re -> {
            if (re.succeeded() && !f.isComplete()) {
                f.complete(re.result());
            } else if (!f.isComplete()) {
                f.fail(re.cause());
            }
        }), resultHandler);
    }

    private void unindexValueBlocking(String id, String fieldName, String indexName, JsonObject instance, AsyncResultHandler<Boolean> resultHandler) {
        try {
            if (cls.getDeclaredField(fieldName).isAnnotationPresent(RedissonIndex.class)) {
                RedissonIndex index = cls.getDeclaredField(fieldName).getAnnotation(RedissonIndex.class);
                unindexValue(id, fieldName, index, instance, resultHandler);
            } else if (cls.getDeclaredField(fieldName).isAnnotationPresent(RedissonIndex.List.class)) {
                RedissonIndex.List indexList = cls.getDeclaredField(fieldName).getAnnotation(RedissonIndex.List.class);
                Arrays.stream(indexList.value()).filter(e -> e.name().equals(indexName)).findFirst().ifPresent(index -> unindexValue(id, fieldName, index, instance, resultHandler));
            }
        } catch (NoSuchFieldException | RuntimeException e) {
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    private void unindexValue(String id, String fieldName, RedissonIndex index, JsonObject instance, AsyncResultHandler<Boolean> resultHandler) {
        try {
            Object value = instance.getValue(fieldName);
            String resolve = index.valueResolver().newInstance().resolve(value, instance, id, fieldName, index);
            GenericFutureListener<io.netty.util.concurrent.Future<Boolean>> futureListener = (io.netty.util.concurrent.Future<Boolean> future) -> {
                resultHandler.handle(future.isSuccess() && future.get() != null ? Future.succeededFuture(future.get()) : Future.failedFuture(future.cause()));
            };
            if (index.scoreResolver().isAssignableFrom(LexicalScoreResolver.class)) {
                redissonOther.getLexSortedSet(getScoredSortedIndexKey(fieldName, index.name())).removeAsync(resolve).addListener(futureListener);
            } else {
                redissonOther.getScoredSortedSet(getScoredSortedIndexKey(fieldName, index.name()), StringCodec.INSTANCE).removeAsync(resolve).addListener(futureListener);
            }
        } catch (InstantiationException | IllegalAccessException | RuntimeException e) {
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    private void prepareEnsuerCounter(String id, T r, JsonObject instance, Boolean isNew, RBatch redissonBatch, RedissonCounter counter, String fieldName, Object fieldValue) throws RuntimeException {
        try {
            if (isNew) {
                redissonBatch.getAtomicLongAsync(getCounterKey(fieldName, counter.name())).incrementAndGetAsync();
            }
            Object value = instance.getValue(fieldName);
            if (counter.expiryResolver().isAssignableFrom(RedisPreciseTimeExpiryResolver.class)) {
                redissonBatch.getAtomicLongAsync(getCounterKey(fieldName, counter.name())).expireAtAsync(counter.expiryResolver().newInstance().resolve(value, instance, id, fieldName, counter));
            } else if (isNew || counter.resetOnUpdate()) {
                redissonBatch.getAtomicLongAsync(getCounterKey(fieldName, counter.name())).expireAsync(counter.expiryResolver().newInstance().resolve(value, instance, id, fieldName, counter), TimeUnit.MILLISECONDS);
            }
        } catch (InstantiationException | IllegalAccessException | RuntimeException e) {
            throw new RuntimeException(e);
        }
    }

    private void reIndexBlocking(String id, T r, JsonObject data, Boolean isNew, Handler<AsyncResult<Void>> resultHandler) {

        RBatch updateIndexBatch = redissonOther.createBatch();
        ArrayList<String> pList = new ArrayList();
        try {
            prepareEnsuerIndex(id, r, data, isNew, updateIndexBatch, pList, null);
        } catch (Exception e) {
            resultHandler.handle(Future.failedFuture(e));
            return;
        }
        updateIndexBatch.executeAsync().addListener(f -> {
            resultHandler.handle(f.isSuccess() ? Future.succeededFuture() : Future.failedFuture(new RepositoryException(f.cause())));
            if (f.isSuccess()) {
                logger.debug("[" + getStorageKey() + "] Re Index Success : " + id);
            } else {
                logger.debug("[" + getStorageKey() + "] Re Index Failure", f.cause());
            }
        });
    }

    private void newId(AsyncResultHandler<String> resultHandler) {
        redissonOther.getAtomicLong(getSequenceKey()).incrementAndGetAsync().addListener(future -> {
            resultHandler.handle(future.isSuccess() ? (future.get() != null ? Future.succeededFuture(future.get().toString()) : Future.failedFuture(new RepositoryException("No sequence returned"))) : Future.failedFuture(new RepositoryException(future.cause())));
            if (future.isSuccess()) {
                logger.debug("[" + getStorageKey() + "] New ID Success : " + future.get().toString());
            } else {
                logger.debug("[" + getStorageKey() + "] New ID Failure", future.cause());
            }
        });
    }

    private T prepareInstanceAndFetchList(String id, RBatch redissonBatch, ArrayList<String> pList, String parent, Boolean deepFetch) throws InstantiationException, IllegalAccessException {
        BeanMap pMap = new BeanMap(cls.newInstance());
        try {
            pMap.keySet().forEach(e -> {
                if ("class".equals(e)) {
                    return;
                }
                Class type = pMap.getType(e.toString());
                String fieldName = (parent == null ? "" : parent.concat(".")).concat(e.toString());
                if (isRedisEntity(type)) {
                    if (deepFetch) {
                        try {
                            RedisRepositoryImpl innerRepo = (RedisRepositoryImpl) factory.instance(type);
                            pMap.put(e.toString(), innerRepo.prepareInstanceAndFetchList(id, redissonBatch, pList, fieldName, deepFetch));
                        } catch (InstantiationException | IllegalAccessException | RepositoryException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                } else {
                    if ("id".equals(e)) {
                        redissonBatch.getMap(getStorageKey(), StringCodec.INSTANCE).getAsync(id);
                    } else {
                        redissonBatch.getMap(getStorageKey(e.toString())).getAsync(id);
                    }
                    pList.add(fieldName);
                }
            });
        } catch (RuntimeException ex) {
            throw new InstantiationException();
        }
        return (T) pMap.getBean();
    }

    private T mapResults(T instance, List results, ArrayList<String> pList) throws InstantiationException, IllegalAccessException, IllegalArgumentException {
        pList.parallelStream().forEach(e -> {
            Object o = instance;
            for (String fieldName : e.split("\\.")) {
                BeanMap m = new BeanMap(o);
                Class type = m.getType(fieldName);
                if (isRedisEntity(type)) {
                    o = m.get(fieldName);
                } else {
                    Object value;
                    Object result = results.get(pList.indexOf(e));
                    if (result == null) {
                        value = null;
                    } else if (String.class.isAssignableFrom(type)) {
                        value = result.toString();
                    } else if (type.isEnum()) {
                        try {
                            value = type.getMethod("valueOf", String.class).invoke(null, results.get(pList.indexOf(e)));
                        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
                            throw new IllegalArgumentException(ex);
                        }
                    } else if (result.getClass().isAssignableFrom(type)) {
                        value = type.cast(result);
                    } else {
                        try {
                            value = type.getConstructor(result.getClass()).newInstance(result);
                        } catch (NoSuchMethodException | SecurityException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
                            throw new IllegalArgumentException(ex);
                        }
                    }
                    m.put(fieldName, value);
                }
            }
        });
        return instance;
    }

    private void deepFetch(String id, AsyncResultHandler<T> resultHandler) {
        fetch(id, Boolean.TRUE, resultHandler);
    }

    private void fetch(String id, Boolean deepFetch, AsyncResultHandler<T> resultHandler) {
        vertx.executeBlocking(f -> {
            if (isRedisEntity()) {
                try {
                    RBatch batch = redissonRead.createBatch();
                    ArrayList<String> pList = new ArrayList();
                    T instance = prepareInstanceAndFetchList(id, batch, pList, null, deepFetch);
                    batch.executeAsync().addListener(future -> {
                        if (future.isSuccess() && !f.isComplete()) {
                            List results = (List) future.get();
                            f.complete(mapResults(instance, results, pList));
                        } else if (!f.isComplete()) {
                            f.fail(new RepositoryException(future.cause()));
                        }
                    });
                } catch (InstantiationException | IllegalAccessException ex) {
                    if (!f.isComplete()) {
                        f.fail((new RepositoryException(ex)));
                    }
                }
            } else {
                redissonRead.<String, String>getMap(getStorageKey(), StringCodec.INSTANCE).getAsync(id).addListener(future -> {
                    if (future.isSuccess() && future.get() != null && !f.isComplete()) {
                        f.complete(Json.decodeValue((String) future.get(), cls));
                        logger.debug("[" + getStorageKey() + "] Fetch Success : " + id + " - " + future.get());
                    } else if (!f.isComplete()) {
                        f.fail(future.get() != null ? new RepositoryException(future.cause()) : new RepositoryException("No search result returned"));
                        logger.debug("[" + getStorageKey() + "] Fetch Failure : " + id, future.cause());
                    }
                });
            }
        }, resultHandler);
    }

    private void persist(String id, JsonObject data, Handler<AsyncResult<Boolean>> resultHandler) {
        persist(id, data, null, resultHandler);
    }

    private void persist(String id, JsonObject data, RBatch redissonBatch, Handler<AsyncResult<Boolean>> resultHandler) {
        vertx.executeBlocking(f -> {
            persistBlocking(id, data, redissonBatch, r -> {
                if (r.succeeded() && !f.isComplete()) {
                    f.complete(r.result());
                } else if (!f.isComplete()) {
                    f.fail(r.cause());
                }
            });
        }, resultHandler);
    }

    private void persistBlocking(String id, JsonObject data, RBatch redissonBatch, Handler<AsyncResult<Boolean>> resultHandler) {
        RBatch batch = redissonBatch == null ? redissonWrite.createBatch() : redissonBatch;
        AtomicBoolean failed = new AtomicBoolean(false);
        try {
            BeanMap pMap = new BeanMap(cls.newInstance());
            //remove the indexes;
            if (isRedisEntity()) {
                AtomicBoolean finished = new AtomicBoolean(false);
                AtomicBoolean hasNested = new AtomicBoolean(false);
                AtomicLong stack = new AtomicLong();
                pMap.forEach((k, v) -> {
                    if ("class".equals(k)) {
                        return;
                    }
                    Class<?> type = pMap.getType((String) k);
                    if (!isRedisEntity(type)) {
                        //recreate the indexes;
                        if ("id".equals(k)) {
                            batch.getMap(getStorageKey(), StringCodec.INSTANCE).fastPutAsync(id, id);
                        } else {
                            batch.getMap(getStorageKey((String) k)).fastPutAsync(id, data.getValue((String) k));
                        }
                    } else {
                        hasNested.set(true);
                        stack.incrementAndGet();
                        RedisRepositoryImpl<?> innerRepo;
                        try {
                            innerRepo = (RedisRepositoryImpl) factory.instance(type);
                        } catch (RepositoryException e) {
                            throw new RuntimeException(e);
                        }
                        JsonObject value = data.getJsonObject((String) k);
                        final boolean newOne = !value.containsKey("id") || value.getString("id") == null || "null".equals(value.getString("id"));
                        final String ID = newOne ? id : value.getString("id");
                        innerRepo.persist(ID, value, batch, c -> {//making the nested entity shares the same id as the parent when its 1:1 relation. This makes fetch a lot faster since it doesn't not need to resolve the reference when fetching 1:1 nested objects.
                            if (c.succeeded()) {
                                long s = stack.decrementAndGet();
                                if (newOne) {
                                    batch.getMap(getStorageKey((String) k)).fastPutAsync(id, ID);//different to the update, create needs to add the reference field to batch
                                }
                                if (s == 0 && finished.get() && !failed.get()) { //finished iterating and no outstanding processes. 
                                    if (redissonBatch == null) {//if it's not inside a nested process.
                                        finishPersist(id, data, batch, resultHandler);
                                    } else {//if it is inside a nested process.
                                        resultHandler.handle(Future.succeededFuture(true));
                                    }
                                }
                                //else wait for others to complete
                            } else {
                                boolean firstToFail = failed.compareAndSet(false, true);
                                if (firstToFail) {
                                    resultHandler.handle(Future.failedFuture(c.cause()));
                                }
                            }
                        });
                    }
                });
                batch.getAtomicLongAsync(getCounterKey()).incrementAndGetAsync();
                finished.set(true);
                if (!hasNested.get()) {//does not have nested RedissonEntity within
                    if (redissonBatch == null) {//if it's not inside a nested process.
                        finishPersist(id, data, batch, resultHandler);
                    } else {//if it is inside a nested process.
                        resultHandler.handle(Future.succeededFuture(true));
                    }
                }
            } else {//not a RedissonEntity class, persist as json string.
                //recreate the indexes;
                batch.<String, String>getMap(getStorageKey(), StringCodec.INSTANCE).fastPutAsync(id, Json.encode(data));
                batch.getAtomicLongAsync(getCounterKey()).incrementAndGetAsync();
                if (redissonBatch == null) {//if it's not inside a nested process.
                    finishPersist(id, data, batch, resultHandler);
                } else {//if it is inside a nested process.
                    resultHandler.handle(Future.succeededFuture(true));
                }
            }
        } catch (InstantiationException | IllegalAccessException | RuntimeException ex) {
            failed.set(true);
            resultHandler.handle(Future.failedFuture(ex));
        }
    }

    private void finishPersist(String id, JsonObject data, RBatch batch, Handler<AsyncResult<Boolean>> resultHandler) {
        vertx.executeBlocking(f -> {
            batch.executeAsync().addListener(future -> {
                if (future.isSuccess()) {
                    if (future.get() != null && !f.isComplete()) {
                        f.complete((Boolean) ((List) future.get()).get(0));
                        logger.debug("[" + getStorageKey() + "] Persist Success : " + id + " - " + Json.encode(data));
                    } else if (!f.isComplete()) {
                        f.fail(new RepositoryException("No save result returned"));
                        logger.debug("[" + getStorageKey() + "] Persist Failure : " + id + " - " + Json.encode(data), future.cause());
                    }
                } else if (!f.isComplete()) {
                    f.fail(new RepositoryException(future.cause()));
                    logger.debug("[" + getStorageKey() + "] Persist Failure : " + id + " - " + Json.encode(data), future.cause());
                }
            });
        }, resultHandler);

    }

    private String lookupIdFromIndex(String indexedValue, String fieldName, String indexName) throws RuntimeException {
        try {
            RedissonIndex index = null;
            Field field = cls.getDeclaredField(fieldName);
            if (field.isAnnotationPresent(RedissonIndex.List.class)) {
                RedissonIndex.List list = field.getAnnotation(RedissonIndex.List.class);
                index = Arrays.stream(list.value()).filter(r
                        -> r.name().equals(indexName)
                ).findFirst().get();
            }
            if (field.isAnnotationPresent(RedissonIndex.class)) {
                index = field.getAnnotation(RedissonIndex.class);
                if (!index.name().equals(indexName)) {
                    throw new NoSuchElementException("Index not found: " + indexName);
                }
            }
            if (index == null) {
                throw new NoSuchElementException();
            }
            return index.valueResolver().newInstance().lookup(indexedValue, index);
        } catch (NoSuchFieldException | SecurityException | InstantiationException | IllegalAccessException | NoSuchElementException e) {
            throw new RuntimeException("Can not find index " + indexName + " for field " + fieldName + " under class " + cls.getCanonicalName() + " to resolve value " + indexedValue, e);
        }
    }

    private boolean isRedisEntity() {
        return isRedisEntity(this.cls);
    }

    private boolean isRedisEntity(Class cls) {
        return cls != null && cls.isAnnotationPresent(RedissonEntity.class);
    }

    private String getSequenceKey() {
        return cls.getCanonicalName().concat(DEFAULT_SEPERATOR).concat(DEFAULT_SEQUENCE_SUFFIX);
    }

    private String getCounterKey() {
        return getInstanceCounterKey(this.cls);
    }

    private String getInstanceCounterKey(Class cls) {
        return cls.getCanonicalName().concat(DEFAULT_SEPERATOR).concat(DEFAULT_COUNTER_SUFFIX);
    }

    private String getStorageKey() {
        return cls.getCanonicalName().concat(DEFAULT_SEPERATOR).concat(DEFAULT_ID_SUFFIX);
    }

    private String getStorageKey(String fieldName) {
        return cls.getCanonicalName().concat(DEFAULT_SEPERATOR).concat(fieldName);
    }

    private String getUniqueKey(String fieldName) {
        return getUniqueKey(cls, fieldName);
    }

    private String getUniqueKey(Class cls, String fieldName) {
        return cls.getCanonicalName().concat(DEFAULT_SEPERATOR).concat(fieldName).concat(DEFAULT_SEPERATOR).concat(DEFAULT_UNIQUE_SUFFIX);
    }

    private String getCounterKey(String fieldName) {
        return getCounterKey(cls, fieldName);
    }

    private String getCounterKey(Class cls, String fieldName) {
        return getCounterKey(cls, fieldName, DEFAULT_INDEX_NAME);
    }

    private String getCounterKey(String fieldName, String counterName) {
        return getCounterKey(cls, fieldName, counterName);
    }

    private String getCounterKey(Class cls, String fieldName, String counterName) {
        return cls.getCanonicalName().concat(DEFAULT_SEPERATOR).concat(fieldName).concat(DEFAULT_SEPERATOR).concat(DEFAULT_COUNTER_SUFFIX).concat(DEFAULT_SEPERATOR).concat(counterName);
    }

    private String getScoredSortedIndexKey(String fieldName) {
        return getScoredSortedIndexKey(cls, fieldName);
    }

    private String getScoredSortedIndexKey(String fieldName, String indexName) {
        return getScoredSortedIndexKey(cls, fieldName, indexName);
    }

    private String getScoredSortedIndexKey(Class cls, String fieldName) {
        return getScoredSortedIndexKey(cls, fieldName, DEFAULT_INDEX_NAME);
    }

    private String getScoredSortedIndexKey(Class cls, String fieldName, String indexName) {
        return cls.getCanonicalName().concat(DEFAULT_SEPERATOR).concat(fieldName).concat(DEFAULT_SEPERATOR).concat(DEFAULT_INDEX_SUFFIX).concat(DEFAULT_SEPERATOR).concat(indexName);
    }
}
