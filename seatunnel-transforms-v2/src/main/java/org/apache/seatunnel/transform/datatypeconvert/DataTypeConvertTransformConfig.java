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

package org.apache.seatunnel.transform.datatypeconvert;

import lombok.Getter;
import lombok.Setter;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.SqlType;

import java.io.Serializable;

@Getter
@Setter
public class DataTypeConvertTransformConfig implements Serializable {

    public static final Option<String> KEY_TRANSFORM_FIELD =
            Options.key("transform_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The field you want to transform");

    public static final Option<SqlType> KEY_TRANSFORM_TYPE =
            Options.key("transform_type")
                    .enumType(SqlType.class)
                    .noDefaultValue()
                    .withDescription("The type you want to transform");
    public static final Option<Integer> KEY_PRECISION =
            Options.key("precision")
                    .intType()
                    .noDefaultValue()
                    .withDescription("精度，有几位有效数字");
    public static final Option<Integer> KEY_SCALE =
            Options.key("scale")
                    .intType()
                    .noDefaultValue()
                    .withDescription("小数点");



    private String transformField;
    private SqlType transformType;
    private int precision;
    private int scale;


    public static DataTypeConvertTransformConfig of(ReadonlyConfig config) {
        DataTypeConvertTransformConfig typeConfig = new DataTypeConvertTransformConfig();
        typeConfig.setTransformField(config.get(KEY_TRANSFORM_FIELD));
        typeConfig.setTransformType(config.get(KEY_TRANSFORM_TYPE));
        typeConfig.setPrecision(config.get(KEY_PRECISION));
        typeConfig.setScale(config.get(KEY_SCALE));
        return typeConfig;
    }
}
