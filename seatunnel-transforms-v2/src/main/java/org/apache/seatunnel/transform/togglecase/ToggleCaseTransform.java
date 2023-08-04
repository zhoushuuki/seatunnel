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

package org.apache.seatunnel.transform.togglecase;

import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;

import java.util.List;
import java.util.Objects;

@AutoService(SeaTunnelTransform.class)
@NoArgsConstructor
public class ToggleCaseTransform extends MultipleFieldOutputTransform {
    private ReadonlyConfig config;

    public ToggleCaseTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return "ToggleCase";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        ConfigValidator.of(ReadonlyConfig.fromConfig(pluginConfig)).validate(new ToggleCaseTransformFactory().optionRule());
        this.config = ReadonlyConfig.fromConfig(pluginConfig);
    }

    @Override
    protected void setInputRowType(SeaTunnelRowType inputRowType) {
    }

    private String[] getFields() {
        String fieldsList = config.get(ToggleCaseTransformConfig.KEY_TOGGLE_CASE_FIELD);
        return fieldsList.split(",");
    }

    @Override
    protected String[] getOutputFieldNames() {
        return getFields();
    }

    @Override
    protected SeaTunnelDataType[] getOutputFieldDataTypes() {
        String[] fields = getFields();
        SeaTunnelDataType[] types = new SeaTunnelDataType[fields.length];
        for (int i = 0; i < fields.length; i++) {
            types[i] = BasicType.STRING_TYPE;
        }
        return types;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        String[] fields = getFields();
        Object[] values = new Object[fields.length];
        SeaTunnelRowType seaTunnelRowType = inputCatalogTable.getTableSchema().toPhysicalRowDataType();
        for (int i = 0; i < fields.length; i++) {
            int index = seaTunnelRowType.indexOf(fields[i]);
            Object inputFieldValue = inputRow.getField(index);
            if (inputFieldValue == null) {
                return null;
            }
            ToggleCaseTransformConfig.ToggleWay toggleWay = config.get(ToggleCaseTransformConfig.KEY_TOGGLE_CASE_WAY);
            String value = inputFieldValue.toString();
            // 大小写转换方式不正确，就不转化，原值处理
            if (Objects.isNull(toggleWay)) {
                values[i] = inputFieldValue;
            }
            // 原值不是英文===>不转换，原值处理
            if (!value.matches(ToggleCaseTransformConfig.PATTERN)) values[i] = value;
            if (toggleWay.equals(ToggleCaseTransformConfig.ToggleWay.UPPER_CASE)) {
                values[i] = value.toUpperCase();
            } else {
                values[i] = value.toLowerCase();
            }
        }

        return values;
    }

    @Override
    protected Column[] getOutputColumns() {
        List<Column> dataList = Lists.newArrayList();
        List<Column> columns = inputCatalogTable.getTableSchema().getColumns();
        String[] fields = getFields();
        for (String field : fields) {
            for (Column column : columns) {
                String sourceName = column.getName();
                if (StringUtils.equals(sourceName, field)) {
                    dataList.add(column);
                }
            }
        }
        return dataList.toArray(new Column[0]);
    }
}
