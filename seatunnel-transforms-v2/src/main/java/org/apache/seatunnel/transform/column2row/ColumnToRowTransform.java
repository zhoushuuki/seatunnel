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

package org.apache.seatunnel.transform.column2row;

import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;

import java.util.List;

@AutoService(SeaTunnelTransform.class)
@NoArgsConstructor
public class ColumnToRowTransform extends AbstractCatalogSupportTransform {
    private ReadonlyConfig config;

    private List<String> baseField;

    public ColumnToRowTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
        baseField = config.get(ColumnToRowTransformConfig.KEY_BASE_FIELD);
    }

    @Override
    public String getPluginName() {
        return "ColumnToRow";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        ConfigValidator.of(ReadonlyConfig.fromConfig(pluginConfig)).validate(new ColumnToRowTransformFactory().optionRule());
        this.config = ReadonlyConfig.fromConfig(pluginConfig);
    }


    @Override
    protected TableSchema transformTableSchema() {
        return null;
       /* List<Column> outputColumns = new ArrayList<>();

        SeaTunnelRowType seaTunnelRowType = inputCatalogTable.getTableSchema().toPhysicalRowDataType();

        String[] fieldNames = seaTunnelRowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            outputColumns.add(inputCatalogTable.getTableSchema().getColumns().get(i).copy());
        }
        outputColumns.add(PhysicalColumn.of(this.config.get(ColumnToRowTransformConfig.KEY_OUTPUT_FIELD), BasicType.INT_TYPE, 0, false, -1, "字段长度"));

        List<ConstraintKey> copyConstraintKeys = inputCatalogTable.getTableSchema().getConstraintKeys().stream().map(ConstraintKey::copy).collect(Collectors.toList());

        PrimaryKey copiedPrimaryKey = inputCatalogTable.getTableSchema().getPrimaryKey() == null ? null : inputCatalogTable.getTableSchema().getPrimaryKey().copy();
        return TableSchema.builder().columns(outputColumns).primaryKey(copiedPrimaryKey).constraintKey(copyConstraintKeys).build();*/
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    @Override
    protected SeaTunnelRowType transformRowType(SeaTunnelRowType inputRowType) {
        /*SeaTunnelDataType[] fieldDataTypes = new SeaTunnelDataType[totalFieldsLength];
        System.arraycopy(inputRowType.getFieldTypes(), 0, fieldDataTypes, 0, sourceFieldsLength);
        fieldDataTypes[sourceFieldsLength] = BasicType.INT_TYPE;
        fields[sourceFieldsLength] = this.config.get(ColumnToRowTransformConfig.KEY_OUTPUT_FIELD);
        return new SeaTunnelRowType(fields, fieldDataTypes);*/
        return null;
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        // 作为基础字段的索引
        List<Integer> matchIndex = Lists.newArrayList();
        SeaTunnelRowType seaTunnelRowType = inputCatalogTable.getTableSchema().toPhysicalRowDataType();
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            String var1 = fieldNames[i];
            for (String var2 : baseField) {
                if (StringUtils.equals(var2, var1)) {
                    matchIndex.add(i);
                }
            }
        }
        return null;
       /* Object[] values = new Object[totalFieldsLength];
        System.arraycopy(inputRow.getFields(), 0, values, 0, sourceFieldsLength);

        SeaTunnelRowType seaTunnelRowType = inputCatalogTable.getTableSchema().toPhysicalRowDataType();
        int fieldLength = 0;
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        for (int i = 0; i < fieldNames.length; i++) {
            if (StringUtils.equals(fieldNames[i], this.config.get(ColumnToRowTransformConfig.KEY_FIELD))) {
                Object value = inputRow.getField(i);
                if (Objects.nonNull(value)) {
                    fieldLength = value.toString().length();
                }

            }
        }
        values[sourceFieldsLength] = fieldLength;
        return new SeaTunnelRow(values);*/
    }
}
