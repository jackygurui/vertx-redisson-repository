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
package com.github.jackygurui.vertxredissonrepository.annotation;

import com.github.jackygurui.vertxredissonrepository.repository.index.DefaultValueResolver;
import com.github.jackygurui.vertxredissonrepository.repository.index.LexicalScoreResolver;
import com.github.jackygurui.vertxredissonrepository.repository.index.RedisIndexScoreResolver;
import com.github.jackygurui.vertxredissonrepository.repository.index.RedisIndexValueResolver;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 *
 * @author Rui Gu
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface RedissonIndex {

    @Target({ElementType.FIELD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface List {

        RedissonIndex[] value();
    }

    String name() default "DEFAULT";

    boolean indexOnSave() default true;

    String scoreField() default "";

    String valueField() default "";

    String[] compoundIndexFields() default {};

    Class<? extends RedisIndexScoreResolver> scoreResolver() default LexicalScoreResolver.class;

    Class<? extends RedisIndexValueResolver> valueResolver() default DefaultValueResolver.class;
}
