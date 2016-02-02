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
import com.github.jackygurui.vertxredissonrepository.repository.type.Customer;
import com.github.pffy.chinese.HanyuPinyin;
import io.vertx.core.AsyncResult;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
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
public class CounterConcurrentWaterfallTest {

    private Logger logger;
    private RedisRepositoryFactory factory;
    private RedisRepository<Customer> customerRepository;
    private Vertx vertx;

    @Before
    public void setUp(TestContext context) throws IOException, RepositoryException {
        logger = LoggerFactory.getLogger(this.getClass());
        vertx = Vertx.vertx();
        factory = RedisRepositoryFactory.create(vertx, new JsonObject().put("serverAddress", "redisdb:32768"));
        customerRepository = factory.instance(Customer.class);
        RedissonProvider.create(new JsonObject().put("serverAddress", "redisdb:32768")).deleteByPattern("com.github.jackygurui.vertxredissonrepository.repository.type.Customer*");
    }

    @Test
    public void test5CounterConcurrentWaterfall(TestContext context) throws Exception {
        Async async = context.async();
        HanyuPinyin.convert("");//warm up
        StopWatch sw = new StopWatch();
        sw.start();
        int records = 100;
        org.simondean.vertx.async.Async
                .<Long>series()
                .task(customerRepository::totalCount)
                .task(t -> {
                    try {
                        JsonNode source = JsonLoader.fromResource("/Customer.json");
                        AtomicLong counter = new AtomicLong(0);
                        IntStream.rangeClosed(1, records).parallel().forEach(e -> {
                            JsonObject clone = new JsonObject(Json.encode(source));
                            clone.getJsonObject("personalDetails").put("phoneNumber", ((Long.parseLong(clone.getJsonObject("personalDetails").getString("phoneNumber")) + 10000 + e) + ""));
                            org.simondean.vertx.async.Async.waterfall().<String>task(tt -> {
                                customerRepository.create(Json.encode(clone), tt);
                            }).<Customer>task((id, tt) -> {
                                customerRepository.get(id, tt);
                            }).run((AsyncResult<Customer> r) -> {
                                long ct = counter.incrementAndGet();
//                logger.info("Counter = " + ct + " | success = " + !r.failed());
                                if (r.succeeded()) {
                                    try {
                                        Customer loaded = r.result();
                                        Customer c = Json.decodeValue(clone.encode(), Customer.class);
                                        c.setId(loaded.getId());
                                        c.getAddressDetails().setId(loaded.getId());
                                        c.getPersonalDetails().setId(loaded.getId());
                                        String encoded = Json.encode(c);
                                        if (!r.result().equals(encoded)) {
                                            logger.info(loaded.getId() + " - SOURCE : " + encoded);
                                            logger.info(loaded.getId() + " - RESULT : " + r.result());
                                        }
                                        context.assertEquals(Json.encode(r.result()), encoded);
                                    } catch (Exception ex) {
                                        t.handle(Future.failedFuture(ex));
                                    }
                                } else {
                                    t.handle(Future.failedFuture(r.cause()));
                                }
                                if (ct == records) {
                                    t.handle(Future.succeededFuture(ct));
                                }
                            });
                        });
                    } catch (IOException e) {
                        t.handle(Future.failedFuture(e));
                    }
                })
                .task(customerRepository::totalCount)
                .run(r -> {
                    if (r.succeeded()) {
                        context.assertEquals(r.result().get(0) + r.result().get(1), r.result().get(2));
                        sw.stop();
                        logger.info("test count: time to count then concurrently save and get " + records + " customer records and count again: " + sw.getTime());
                        async.complete();
                    } else {
                        context.fail(r.cause());
                        async.complete();
                    }
                });
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
        factory.shutdown();
    }
}
