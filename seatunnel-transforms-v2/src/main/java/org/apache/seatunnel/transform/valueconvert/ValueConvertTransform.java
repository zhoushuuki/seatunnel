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

package org.apache.seatunnel.transform.valueconvert;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.SingleFieldOutputTransform;

import java.util.List;
import java.util.stream.Collectors;

@AutoService(SeaTunnelTransform.class)
@NoArgsConstructor
public class ValueConvertTransform extends SingleFieldOutputTransform {
    private ReadonlyConfig config;

    public ValueConvertTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return "ValueConvert";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        ConfigValidator.of(ReadonlyConfig.fromConfig(pluginConfig)).validate(new ValueConvertTransformFactory().optionRule());
        this.config = ReadonlyConfig.fromConfig(pluginConfig);
    }

    @Override
    protected void setInputRowType(SeaTunnelRowType inputRowType) {
    }

    @Override
    protected String getOutputFieldName() {
        return this.config.get(ValueConvertTransformConfig.KEY_TRANSFORM_FIELD);
    }

    @Override
    protected SeaTunnelDataType getOutputFieldDataType() {
        return inputCatalogTable.getTableSchema().toPhysicalRowDataType();
    }

    @Override
    protected Object getOutputFieldValue(SeaTunnelRowAccessor inputRow) {
        String transformField = this.config.get(ValueConvertTransformConfig.KEY_TRANSFORM_FIELD);
        SeaTunnelRowType rowType = inputCatalogTable.getTableSchema().toPhysicalRowDataType();
        String[] fieldNames = rowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            String name = fieldNames[i];
            if (StringUtils.equals(name, transformField)) {
                return this.config.get(ValueConvertTransformConfig.KEY_TRANSFORM_VALUE);
            }
        }
        return null;
    }

    @Override
    protected Column getOutputColumn() {
        List<Column> columns = inputCatalogTable.getTableSchema().getColumns();
        List<Column> collect = columns.stream().filter(column -> column.getName().equals(config.get(ValueConvertTransformConfig.KEY_TRANSFORM_FIELD))).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(collect)) {
            throw new IllegalArgumentException("Cannot find [" + config.get(ValueConvertTransformConfig.KEY_TRANSFORM_FIELD) + "] field in input catalog table");
        }
        return collect.get(0).copy();
    }
}
