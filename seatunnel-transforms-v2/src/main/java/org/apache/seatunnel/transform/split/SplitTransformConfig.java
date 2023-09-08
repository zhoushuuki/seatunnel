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

package org.apache.seatunnel.transform.split;

import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

@Getter
@Setter
public class SplitTransformConfig implements Serializable {
    public static final Option<String> KEY_SEPARATOR =
            Options.key("separator")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The separator to split the field");

    public static final Option<SplitType> KEY_SPLIT_TYPE =
            Options.key("split_type")
                    .enumType(SplitType.class)
                    .noDefaultValue()
                    .withDescription("拆分方式");

    public static final Option<String> KEY_SPLIT_FIELD =
            Options.key("split_field")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The field to be split");

    public static final Option<List<String>> KEY_OUTPUT_FIELDS =
            Options.key("output_fields")
                    .listType()
                    .noDefaultValue()
                    .withDescription("The result fields after split");

    private String separator;
    private String splitField;
    private SplitType splitType;
    private String[] outputFields;
    private String[] emptySplits;

    public static SplitTransformConfig of(ReadonlyConfig config) {
        SplitTransformConfig splitTransformConfig = new SplitTransformConfig();
        splitTransformConfig.setSeparator(config.get(KEY_SEPARATOR));
        splitTransformConfig.setSplitField(config.get(KEY_SPLIT_FIELD));
        splitTransformConfig.setSplitType(config.get(KEY_SPLIT_TYPE));
        splitTransformConfig.setOutputFields(config.get(KEY_OUTPUT_FIELDS).toArray(new String[0]));
        splitTransformConfig.setEmptySplits(
                new String[splitTransformConfig.getOutputFields().length]);
        return splitTransformConfig;
    }

    public enum SplitType{
        SEPARATOR_SIGN("separatorSign"),
        BITWISE_SIGN("bitwiseSign"),
        ;
        private final String type_;

        SplitType(String type_) {
            this.type_ = type_;
        }

        public String getType_() {
            return type_;
        }

        public static SplitType getType(String type) {
            return Arrays.stream(SplitType.values()).filter(item -> StringUtils.equals(type, item.getType_())).findFirst().orElse(null);
        }
    }
}
