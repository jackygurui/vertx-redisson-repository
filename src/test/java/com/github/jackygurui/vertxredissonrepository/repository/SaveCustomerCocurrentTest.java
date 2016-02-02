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
public class SaveCustomerCocurrentTest {

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
    public void test3SaveCustomerConcurrent(TestContext context) throws Exception {
        Async async = context.async();
        JsonNode source = JsonLoader.fromResource("/Customer.json");
        int records = 10000;
        HanyuPinyin.convert("");//warm up
        AtomicLong c = new AtomicLong(0);
        StopWatch sw = new StopWatch();
        sw.start();
        IntStream.rangeClosed(1, records).parallel().forEach(e -> {
            JsonObject clone = new JsonObject(Json.encode(source));
            clone.getJsonObject("personalDetails").put("phoneNumber", ((Long.parseLong(clone.getJsonObject("personalDetails").getString("phoneNumber")) + 100 + e) + ""));
            customerRepository.create(Json.encode(clone), r -> {
                AsyncResult<String> result = (AsyncResult<String>) r;
                if (result.failed()) {
                    context.fail(result.cause());
                    async.complete();
                }
                if (c.incrementAndGet() == records) {
                    sw.stop();
                    logger.info("time to concurrently save " + records + " customer records: " + sw.getTime());
                    async.complete();
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
