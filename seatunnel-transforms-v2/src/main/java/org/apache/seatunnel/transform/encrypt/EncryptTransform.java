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

import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;

@AutoService(SeaTunnelTransform.class)
@NoArgsConstructor
public class EncryptTransform extends MultipleFieldOutputTransform {
    private ReadonlyConfig config;

    public EncryptTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return "Encrypt";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        ConfigValidator.of(ReadonlyConfig.fromConfig(pluginConfig)).validate(new EncryptTransformFactory().optionRule());
        this.config = ReadonlyConfig.fromConfig(pluginConfig);
    }

    @Override
    protected void setInputRowType(SeaTunnelRowType inputRowType) {
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
                values[i] = null;
                continue;
            }
            EncryptTransformConfig.EncryptType encryptType = config.get(EncryptTransformConfig.KEY_ENCRYPT_TYPE);
            String value = inputFieldValue.toString();
            // 加密方式不存在，原值处理，空字符串->原值处理
            if (Objects.isNull(encryptType) || StringUtils.isBlank(value)) {
                values[i] = inputFieldValue;
            } else {
                values[i] = encryptValue(value, encryptType);
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

    private String[] getFields() {
        List<String> fieldList = config.get(EncryptTransformConfig.KEY_ENCRYPT_FIELD);
        return fieldList.toArray(new String[0]);
    }

    private Object encryptValue(String inputFieldValue, EncryptTransformConfig.EncryptType encryptType) {
        String packageName = "org.apache.seatunnel.transform.encrypt.utils.";
        String type = encryptType.getType();
        String clazzName = packageName + type;
        try {
            Class<?> clazz = Class.forName(clazzName);
            Method method = clazz.getMethod("encrypt", String.class);
            return method.invoke(null, inputFieldValue);
        } catch (Exception e) {
            return inputFieldValue;
        }
    }
}
