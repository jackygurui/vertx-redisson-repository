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
package com.github.jackygurui.vertxredissonrepository.handler.Impl;

import com.github.jackygurui.vertxredissonrepository.handler.CallInMessageHandler;
import com.github.jackygurui.vertxredissonrepository.repository.RedisRepository;
import com.github.jackygurui.vertxredissonrepository.repository.RedisRepositoryFactory;
import com.github.jackygurui.vertxredissonrepository.repository.RepositoryException;
import com.github.jackygurui.vertxredissonrepository.repository.type.CallIn;
import com.github.jackygurui.vertxredissonrepository.repository.type.Customer;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.simondean.vertx.async.Async;

/**
 *
 * @author Rui Gu
 */
public class CallInMessageHandlerImpl implements CallInMessageHandler {

    private final Vertx vertx;
    private final RedisRepository<CallIn> callInRepo;
    private final RedisRepository<Customer.PersonalDetails> personalDetailsRepo;
    private final RedisRepository<Customer.AddressDetails> addressDetailsRepo;
    private final Map<String, CallIn.Type> numberTypeMap;

    public CallInMessageHandlerImpl(Vertx vertx, RedisRepositoryFactory factory, Map<String, CallIn.Type> numberTypeMap) throws RepositoryException {
        this.vertx = vertx;
        this.callInRepo = factory.instance(CallIn.class);
        this.personalDetailsRepo = factory.instance(Customer.PersonalDetails.class);
        this.addressDetailsRepo = factory.instance(Customer.AddressDetails.class);
        this.numberTypeMap = numberTypeMap;
    }

    @Override
    public void handle(Message<JsonObject> m) {
        JsonObject jBody = m.body();
        String from = jBody.getString("from");
        String to = jBody.getString("to");
        Long requestTime = jBody.getLong("requestTime");
        String userData = jBody.getString("userData");
        AtomicReference<String> cId = new AtomicReference();
        AtomicReference<Customer.PersonalDetails> p = new AtomicReference();
        Async.waterfall().<String>task(t -> {
            personalDetailsRepo.searchUniqueIndex("phoneNumber", from, t);
        }).<Customer.PersonalDetails>task((id, t) -> {
            cId.set(id);
            personalDetailsRepo.get(id, t);
        }).<Customer.AddressDetails>task((c, t) -> {
            p.set(c);
            addressDetailsRepo.get(cId.get(), t);
        }).run(r -> {
            CallIn ci;
            if (r.failed()) {
                ci = CallIn.builder()
                        .callTime(requestTime)
                        .callType(numberTypeMap.get(to))
                        .comments(userData)
                        .fullName("未注册号码")
                        .phoneNumber(from)
                        .build();
            } else {
                Customer.PersonalDetails cpd = p.get();
                Customer.AddressDetails cad = r.result();
                ci = CallIn.builder()
                        .address(cad.getFullAddress())
                        .area(cpd.getArea())
                        .birthDay(cpd.getBirthDay())
                        .callTime(requestTime)
                        .callType(numberTypeMap.get(to))
                        .city(cpd.getCity())
                        .comments(userData)
                        .customerId(cId.get())
                        .fullName(cpd.getFullName())
                        .gender(cpd.getGender())
                        .phoneNumber(from)
                        .province(cpd.getProvince())
                        .type(cpd.getType())
                        .build();
            }
            callInRepo.create(Json.encode(ci), c -> {
                m.reply(c.result());
                ci.setId(c.result());
                vertx.eventBus().publish("client.notification.inComingCall", Json.encode(ci));
            });
        });
    }

}
