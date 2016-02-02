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

import com.github.jackygurui.vertxredissonrepository.annotation.JsonSchemaValid;
import com.github.jackygurui.vertxredissonrepository.annotation.RedissonEntity;
import com.github.jackygurui.vertxredissonrepository.annotation.RedissonIndex;
import com.github.jackygurui.vertxredissonrepository.annotation.RedissonUnique;
import com.github.jackygurui.vertxredissonrepository.repository.index.LexicalPinYinValueResolver;
import com.github.reinert.jjschema.Attributes;
import lombok.Data;

/**
 *
 * @author Rui Gu
 */
@Data
@Attributes
@RedissonEntity
@JsonSchemaValid
public class Customer {

    @Attributes
    private String id;
    @Attributes(required = true)
    private PersonalDetails personalDetails;
    @Attributes(required = true)
    private AddressDetails addressDetails;
    @Attributes(required = true)
    private Boolean termsAgreed;

    @Data
    @RedissonEntity
    static public class PersonalDetails {

        @Attributes
        private String id;
        @Attributes(required = true, minimum = 0, maximum = 1)
        private Integer type = 0;
        @Attributes(required = true, minimum = 0, maximum = 1)
        private Integer gender = 0;
        @Attributes(required = true, minLength = 2, maxLength = 10)
        @RedissonIndex(name = "PINYIN", valueResolver = LexicalPinYinValueResolver.class)
        private String fullName;
        @Attributes(required = true, minLength = 10, maxLength = 10, pattern = "^((19|20)\\d\\d)-(0?[1-9]|1[012])-(0?[1-9]|[12][0-9]|3[01])$")
        private String birthDay;
        @Attributes(required = true, pattern = "(^(([0\\+]\\d{2,3})?(0\\d{2,3}))(\\d{7,8})((\\d{3,}))?$)|(^0{0,1}1[3|4|5|6|7|8|9][0-9]{9}$)")
        @RedissonUnique
        private String phoneNumber;
        @Attributes(required = true, minLength = 6, maxLength = 6, pattern = "^(8?[12]|[1-7][0-7])(0000)$")
        private String province;
        @Attributes(required = true, minLength = 6, maxLength = 6, pattern = "^(8?[12]|[1-7][0-7])(\\d\\d)(00)$")
        private String city;
        @Attributes(required = true, minLength = 6, maxLength = 6, pattern = "^(8?[12]|[1-7][0-7])(\\d\\d)(\\d\\d)$")
        private String area;
    }

    @Data
    @RedissonEntity
    static public class AddressDetails {

        @Attributes
        private String id;
        @Attributes(required = true, minLength = 2, maxLength = 500)
        private String fullAddress;
        @Attributes(required = false, minLength = 0, maxLength = 50)
        private String street;
        @Attributes(required = false, minLength = 0, maxLength = 50)
        private String community;
        @Attributes(required = false, minLength = 0, maxLength = 50)
        private String building;
    }

}
