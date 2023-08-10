/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.transform.encrypt;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class EncryptTransformConfig implements Serializable {

    public static final Option<List<String>> KEY_ENCRYPT_FIELD =
            Options.key("encrypt_field")
                    .listType()
                    .noDefaultValue()
                    .withDescription("要加密的字段");

    public static final Option<EncryptType> KEY_ENCRYPT_TYPE =
            Options.key("encrypt_type")
                    .enumType(EncryptType.class)
                    .noDefaultValue()
                    .withDescription("The value you want to transform");


    public enum EncryptType{
        AES("AES"),
        DES("TRIPLE_DES"),
        SHA("SHA"),
        SM4("SM4"),
        MD5("MD5"),
        BASE64("BASE64"),
        ;


        private final String type;

        EncryptType(String type) {
            this.type = type;
        }

        public String getType() {
            return type;
        }

        public static EncryptType getType(String type) {
            return Arrays.stream(EncryptType.values()).filter(item -> StringUtils.equals(type, item.getType())).findFirst().orElse(null);
        }
    }
}
