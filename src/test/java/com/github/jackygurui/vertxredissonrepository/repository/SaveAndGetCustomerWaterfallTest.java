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
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 *
 * @author Rui Gu
 */
@RunWith(VertxUnitRunner.class)
public class SaveAndGetCustomerWaterfallTest {

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
    public void test2SaveAndGetCustomerWaterfall(TestContext context) throws Exception {
        Async async = context.async();
        final JsonNode source = JsonLoader.fromResource("/Customer.json");
        JsonObject clone = new JsonObject(Json.encode(source));
        clone.getJsonObject("personalDetails").put("phoneNumber", ((Long.parseLong(clone.getJsonObject("personalDetails").getString("phoneNumber")) + 10) + ""));
        org.simondean.vertx.async.Async.waterfall().<String>task(t -> {
            customerRepository.create(Json.encode(clone), t);
        }).<Customer>task((id, t) -> {
            customerRepository.get(id, t);
        }).run(r -> {
            if (r.failed()) {
                context.fail(r.cause());
            } else {
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
                    context.fail(r.cause());
                }
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
