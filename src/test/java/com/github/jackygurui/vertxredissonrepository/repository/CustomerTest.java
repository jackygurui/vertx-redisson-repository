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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonValidator;
import com.github.jackygurui.vertxredissonrepository.repository.type.Customer;
import com.github.reinert.jjschema.exception.UnavailableVersion;
import com.github.reinert.jjschema.v1.JsonSchemaFactory;
import com.github.reinert.jjschema.v1.JsonSchemaV4Factory;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.io.IOException;
import junit.framework.TestCase;

/**
 *
 * @author Rui Gu
 */
public class CustomerTest extends TestCase {

    private Logger logger;
    ObjectWriter om = new ObjectMapper().writerWithDefaultPrettyPrinter();
    JsonSchemaFactory schemaFactory = new JsonSchemaV4Factory();
    private static final JsonValidator VALIDATOR = com.github.fge.jsonschema.main.JsonSchemaFactory.byDefault().getValidator();

    {
        schemaFactory.setAutoPutDollarSchema(true);
    }

    public void testCustomerSchema() throws UnavailableVersion, IOException, ProcessingException {
        logger = LoggerFactory.getLogger(this.getClass());
        JsonNode customerSchema = schemaFactory.createSchema(Customer.class);
        JsonNode source = JsonLoader.fromResource("/Customer.json");
        logger.info(om.writeValueAsString(customerSchema));
        logger.info(om.writeValueAsString(schemaFactory.createSchema(Customer.AddressDetails.class)));
        ProcessingReport r = VALIDATOR.validate(customerSchema, source);
        r.iterator().forEachRemaining(re -> {
            logger.info(re.getMessage());
            try {
                logger.info(om.writeValueAsString(re.asJson()));
            } catch (JsonProcessingException ex) {
                logger.error(null, ex);
            }
        });
        assertTrue(r.isSuccess());
    }
}
