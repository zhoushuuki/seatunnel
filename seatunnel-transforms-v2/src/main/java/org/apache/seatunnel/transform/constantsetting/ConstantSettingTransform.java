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

package org.apache.seatunnel.transform.constantsetting;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.*;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@AutoService(SeaTunnelTransform.class)
@NoArgsConstructor
public class ConstantSettingTransform extends AbstractCatalogSupportTransform {
    private ReadonlyConfig config;

    private String[] fields;

    private int sourceFieldsLength;
    private int totalFieldsLength;

    public ConstantSettingTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
        sourceFieldsLength = inputCatalogTable.getTableSchema().getColumns().size();
        totalFieldsLength = sourceFieldsLength + 1;
        fields = new String[totalFieldsLength];
    }

    @Override
    public String getPluginName() {
        return "ConstantSetting";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        ConfigValidator.of(ReadonlyConfig.fromConfig(pluginConfig)).validate(new ConstantSettingTransformFactory().optionRule());
        this.config = ReadonlyConfig.fromConfig(pluginConfig);
    }


    @Override
    protected TableSchema transformTableSchema() {
        List<Column> outputColumns = new ArrayList<>();

        SeaTunnelRowType seaTunnelRowType = inputCatalogTable.getTableSchema().toPhysicalRowDataType();

        String[] fieldNames = seaTunnelRowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            outputColumns.add(inputCatalogTable.getTableSchema().getColumns().get(i).copy());
        }
        outputColumns.add(PhysicalColumn.of(this.config.get(ConstantSettingTransformConfig.KEY_OUTPUT_FIELD), BasicType.STRING_TYPE, 255, false, "", "设置常量"));

        List<ConstraintKey> copyConstraintKeys = inputCatalogTable.getTableSchema().getConstraintKeys().stream().map(ConstraintKey::copy).collect(Collectors.toList());

        PrimaryKey copiedPrimaryKey = inputCatalogTable.getTableSchema().getPrimaryKey() == null ? null : inputCatalogTable.getTableSchema().getPrimaryKey().copy();
        return TableSchema.builder().columns(outputColumns).primaryKey(copiedPrimaryKey).constraintKey(copyConstraintKeys).build();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    @Override
    protected SeaTunnelRowType transformRowType(SeaTunnelRowType inputRowType) {

        SeaTunnelDataType[] fieldDataTypes = new SeaTunnelDataType[totalFieldsLength];
        System.arraycopy(inputRowType.getFieldTypes(), 0, fieldDataTypes, 0, sourceFieldsLength);
        fieldDataTypes[sourceFieldsLength] = BasicType.INT_TYPE;
        fields[sourceFieldsLength] = this.config.get(ConstantSettingTransformConfig.KEY_OUTPUT_FIELD);
        return new SeaTunnelRowType(fields, fieldDataTypes);
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        Object[] values = new Object[totalFieldsLength];
        System.arraycopy(inputRow.getFields(), 0, values, 0, sourceFieldsLength);

        values[sourceFieldsLength] = this.config.get(ConstantSettingTransformConfig.KEY_CONSTANT_VALUE);
        return new SeaTunnelRow(values);
    }
}
