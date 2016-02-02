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

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.jackygurui.vertxredissonrepository.provider.RedissonProvider;
import com.github.jackygurui.vertxredissonrepository.repository.type.CallIn;
import com.github.jackygurui.vertxredissonrepository.repository.type.Customer;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 *
 * @author Rui Gu
 */
@RunWith(VertxUnitRunner.class)
public class SaveAndSearchAndGetCallInConcurrentTest {

    private Logger logger;
    private RedisRepositoryFactory factory;
    private RedisRepository<Customer> customerRepository;
    private RedisRepository<CallIn> callInRepository;
    private Vertx vertx;

    @Before
    public void setUp(TestContext context) throws IOException, RepositoryException {
        logger = LoggerFactory.getLogger(this.getClass());
        vertx = Vertx.vertx();
        factory = RedisRepositoryFactory.create(vertx, new JsonObject().put("serverAddress", "redisdb:32768"));
        customerRepository = factory.instance(Customer.class);
        callInRepository = factory.instance(CallIn.class);
        RedissonProvider.create(new JsonObject().put("serverAddress", "redisdb:32768")).deleteByPattern("com.github.jackygurui.vertxredissonrepository.repository.type.CallIn*");
    }

    @Test
    public void test2SaveAndSearchAndGetCallIn(TestContext context) throws Exception {
        Async async = context.async();
        JsonNode source = JsonLoader.fromResource("/CallIn.json");
        int records = 1000;
        AtomicLong total = new AtomicLong(0);
        ConcurrentHashMap<JsonObject, String> m = new ConcurrentHashMap<>();
        Stream<JsonObject> stream = IntStream.rangeClosed(0, records).mapToObj(e -> {
            JsonObject clone = new JsonObject(Json.encode(source));
            Long number = Long.parseLong(clone.getString("phoneNumber")) + e;
            clone.put("phoneNumber", number + "");
            Long callTime = clone.getLong("callTime") + e;
            clone.put("callTime", callTime);
            return clone;
        });
        StopWatch sw = new StopWatch();
        sw.start();
        stream.parallel().forEach(e -> {
            org.simondean.vertx.async.Async.waterfall()
                    .<String>task(t -> {
                        callInRepository.create(Json.encode(e), t);
                    }).<List<CallIn>>task((id, t) -> {
                        m.put(e, id);
                        AtomicLong idc = new AtomicLong(0);
                        org.simondean.vertx.async.Async.retry().<List<CallIn>>task(tt -> {
                            callInRepository.searchIndexByScoreAndGet("callTime", e.getDouble("callTime"), e.getDouble("callTime"), 0, 1, ttt -> {
                                logger.info("id = " + id + " | retry count: " + idc.incrementAndGet());
                                tt.handle(ttt.succeeded() && ttt.result() != null && !ttt.result().isEmpty() ? Future.succeededFuture(ttt.result()) : Future.failedFuture(ttt.cause()));
                            });
                        }).times(100000).run(t);
                    }).run(r -> {
                        context.assertTrue(r.succeeded());
                        if (r.succeeded()) {
                            context.assertFalse(r.result().isEmpty());
                            context.assertEquals(1, r.result().size());
                            CallIn ci = r.result().iterator().next();
                            context.assertNotNull(ci);
                            logger.info(Json.encode(ci));
                            CallIn cii = Json.decodeValue(e.put("id", m.get(e)).encode(), CallIn.class);
                            context.assertEquals(Json.encode(cii), Json.encode(ci));
                        }
                        long t;
                        if ((t = total.incrementAndGet()) == records) {
                            sw.stop();
                            logger.info("time to concurrently save and search and get " + records + " call in records: " + sw.getTime());
                            async.complete();
                        } else {
                            logger.info("t = " + t);
                        }
                    });
        });

    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
        factory.shutdown();
    }
}
