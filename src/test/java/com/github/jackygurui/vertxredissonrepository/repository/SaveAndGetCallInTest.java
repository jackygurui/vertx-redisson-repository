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
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 *
 * @author Rui Gu
 */
@RunWith(VertxUnitRunner.class)
public class SaveAndGetCallInTest {

    private Logger logger;
    private RedisRepositoryFactory factory;
    private RedisRepository<CallIn> callInRepository;
    private Vertx vertx;

    @Before
    public void setUp(TestContext context) throws IOException, RepositoryException {
        logger = LoggerFactory.getLogger(this.getClass());
        vertx = Vertx.vertx();
        factory = RedisRepositoryFactory.create(vertx, new JsonObject().put("serverAddress", "redisdb:32768"));
        callInRepository = factory.instance(CallIn.class);
        RedissonProvider.create(new JsonObject().put("serverAddress", "redisdb:32768")).deleteByPattern("com.github.jackygurui.vertxredissonrepository.repository.type.CallIn*");
    }

    @Test
    public void test1SaveAndGetCallIn(TestContext context) throws Exception {
        Async async = context.async();
        JsonNode source = JsonLoader.fromResource("/CallIn.json");
        JsonObject clone = new JsonObject(Json.encode(source));
        Long number = Long.parseLong(clone.getString("phoneNumber")) + 1;
        clone.put("phoneNumber", number + "");
        AtomicReference ar = new AtomicReference();
        org.simondean.vertx.async.Async.waterfall()
                .<String>task(t -> {
                    callInRepository.create(Json.encode(clone), t);
                }).<CallIn>task((id, t) -> {
                    ar.set(id);
                    callInRepository.get(id, t);
                }).run(r -> {
                    context.assertTrue(r.succeeded());
                    if (r.succeeded()) {
                        logger.info(r.result());
                        CallIn cii = Json.decodeValue(clone.put("id", ar.get()).encode(), CallIn.class);
                        context.assertEquals(Json.encode(cii), Json.encode(r.result()));
                    }
                    async.complete();
                });
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
        factory.shutdown();
    }
}
