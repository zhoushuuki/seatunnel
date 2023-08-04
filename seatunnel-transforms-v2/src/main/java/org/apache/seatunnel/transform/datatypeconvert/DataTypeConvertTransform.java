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

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.*;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.common.SingleFieldOutputTransform;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@AutoService(SeaTunnelTransform.class)
@NoArgsConstructor
public class DataTypeConvertTransform extends SingleFieldOutputTransform {
    private DataTypeConvertTransformConfig config;

    private int inputFieldIndex;

    public DataTypeConvertTransform(@NonNull DataTypeConvertTransformConfig config, @NonNull CatalogTable inputCatalogTable) {
        super(inputCatalogTable);
        this.config = config;
        initOutputFields(inputCatalogTable.getTableSchema().toPhysicalRowDataType(), config.getTransformField());
    }

    @Override
    public String getPluginName() {
        return "DataTypeConvert";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        ConfigValidator.of(ReadonlyConfig.fromConfig(pluginConfig)).validate(new DataTypeConvertTransformFactory().optionRule());
        this.config = DataTypeConvertTransformConfig.of(ReadonlyConfig.fromConfig(pluginConfig));
    }

    @Override
    protected void setInputRowType(SeaTunnelRowType inputRowType) {
        initOutputFields(inputRowType, this.config.getTransformField());
    }

    private void initOutputFields(SeaTunnelRowType inputRowType, String transformField) {
        inputFieldIndex = inputRowType.indexOf(transformField);
        if (inputFieldIndex == -1) {
            throw new IllegalArgumentException("Cannot find [" + transformField + "] field in input row type");
        }
    }

    @Override
    protected String getOutputFieldName() {
        return this.config.getTransformField();
    }

    @Override
    protected SeaTunnelDataType getOutputFieldDataType() {
        SqlType sqlType = this.config.getTransformType();
        if (Objects.isNull(handleType(sqlType))) throw new UnsupportedOperationException("转换类型错误!");
        return handleType(sqlType);
    }

    private SeaTunnelDataType handleType(SqlType sqlType){
        SeaTunnelDataType type = null;
        switch (sqlType){
            case STRING:
                type = BasicType.STRING_TYPE;
                break;
            case INT:
                type =  BasicType.INT_TYPE;
                break;
            case BOOLEAN:
                type =  BasicType.BOOLEAN_TYPE;
                break;
            case BYTES:
                type =  BasicType.BYTE_TYPE;
                break;
            case TINYINT:
            case SMALLINT:
                type =  BasicType.SHORT_TYPE;
                break;
            case BIGINT:
                type =  BasicType.LONG_TYPE;
                break;
            case FLOAT:
                type =  BasicType.FLOAT_TYPE;
                break;
            case DOUBLE:
                type =  BasicType.DOUBLE_TYPE;
                break;
            case DECIMAL:
                type =  new DecimalType(config.getPrecision(), config.getScale());
        }
        return type;
    }
    
    private Object handleValue(SqlType sqlType, Object value){
        Object temp = null;
        if (Objects.isNull(value)) return null;
        try{
            String var1 = value.toString();
            switch (sqlType){
                case STRING:
                    temp = var1;
                    break;
                case INT:
                    temp =  Integer.parseInt(var1);
                    break;
                case BOOLEAN:
                    temp = Boolean.valueOf(var1);
                    break;
                case BYTES:
                    temp = var1.getBytes();
                    break;
                case TINYINT:
                case SMALLINT:
                    temp =  Short.valueOf(var1);
                    break;
                case BIGINT:
                    temp =  Long.valueOf(var1);
                    break;
                case FLOAT:
                    temp =  Float.valueOf(var1);
                    break;
                case DOUBLE:
                    temp =  Double.valueOf(var1);
                    break;
                case DECIMAL:
                    MathContext mathContext = new MathContext(config.getPrecision());
                    BigDecimal bigDecimal = new BigDecimal(var1, mathContext);
                    temp = bigDecimal.setScale(config.getScale(), RoundingMode.HALF_UP);

            }
        }catch(Exception e){
            // 出现异常的话，原值处理
            temp = value;
        }

        return temp;

    }
    @Override
    protected Object getOutputFieldValue(SeaTunnelRowAccessor inputRow) {
        Object value = inputRow.getField(inputFieldIndex);
        return handleValue(config.getTransformType(), value);
    }

    @Override
    protected Column getOutputColumn() {
        List<Column> columns = inputCatalogTable.getTableSchema().getColumns();
        List<Column> collect = columns.stream().filter(column -> column.getName().equals(config.getTransformField())).collect(Collectors.toList());
        if (CollectionUtils.isEmpty(collect)) {
            throw new IllegalArgumentException("Cannot find [" + config.getTransformField() + "] field in input catalog table");
        }
        Column column = collect.get(0).copy();
        return PhysicalColumn.of(column.getName(), getOutputFieldDataType(), column.getColumnLength(), true, column.getDefaultValue(), column.getComment());

    }
}
