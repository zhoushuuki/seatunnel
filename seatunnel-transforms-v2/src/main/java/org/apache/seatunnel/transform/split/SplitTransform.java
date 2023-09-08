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

import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;

import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@AutoService(SeaTunnelTransform.class)
@NoArgsConstructor
public class SplitTransform extends MultipleFieldOutputTransform {
    private SplitTransformConfig splitTransformConfig;
    private int splitFieldIndex;

    public SplitTransform(
            @NonNull SplitTransformConfig splitTransformConfig,
            @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.splitTransformConfig = splitTransformConfig;
        SeaTunnelRowType seaTunnelRowType = catalogTable.getTableSchema().toPhysicalRowDataType();
        splitFieldIndex = seaTunnelRowType.indexOf(splitTransformConfig.getSplitField());
        if (splitFieldIndex == -1) {
            throw new IllegalArgumentException(
                    "Cannot find ["
                            + splitTransformConfig.getSplitField()
                            + "] field in input row type");
        }
        this.outputCatalogTable = getProducedCatalogTable();
    }

    @Override
    public String getPluginName() {
        return "Split";
    }

    @Override
    protected void setConfig(Config pluginConfig) {
        ConfigValidator.of(ReadonlyConfig.fromConfig(pluginConfig))
                .validate(new SplitTransformFactory().optionRule());
        this.splitTransformConfig =
                SplitTransformConfig.of(ReadonlyConfig.fromConfig(pluginConfig));
    }

    @Override
    protected void setInputRowType(SeaTunnelRowType rowType) {
        splitFieldIndex = rowType.indexOf(splitTransformConfig.getSplitField());
        if (splitFieldIndex == -1) {
            throw new IllegalArgumentException(
                    "Cannot find ["
                            + splitTransformConfig.getSplitField()
                            + "] field in input row type");
        }
    }

    @Override
    protected String[] getOutputFieldNames() {
        return splitTransformConfig.getOutputFields();
    }

    @Override
    protected SeaTunnelDataType[] getOutputFieldDataTypes() {
        return IntStream.range(0, splitTransformConfig.getOutputFields().length)
                .mapToObj((IntFunction<SeaTunnelDataType>) value -> BasicType.STRING_TYPE)
                .toArray(value -> new SeaTunnelDataType[value]);
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        Object splitFieldValue = inputRow.getField(splitFieldIndex);
        if (splitFieldValue == null) {
            return splitTransformConfig.getEmptySplits();
        }
        SplitTransformConfig.SplitType splitType = splitTransformConfig.getSplitType();
        String[] outputFields = splitTransformConfig.getOutputFields();
        String separator = splitTransformConfig.getSeparator();
        int length = outputFields.length;
        String[] splitFieldValues = new String[length];
        if (splitType != null) {
            String stringValue = splitFieldValue.toString();
            if (splitType == SplitTransformConfig.SplitType.SEPARATOR_SIGN) { // 字符分割
                splitFieldValues = stringValue.split(separator, splitTransformConfig.getOutputFields().length);
                if (splitFieldValues.length < length) {
                    String[] tmp = splitFieldValues;
                    splitFieldValues = new String[length];
                    System.arraycopy(tmp, 0, splitFieldValues, 0, tmp.length);
                }
            } else if (splitType == SplitTransformConfig.SplitType.BITWISE_SIGN) { // 按位拆分
                List<Integer> indexList = parseSeparator(separator);
                int strLength = stringValue.length();
                int max = indexList.stream().max(Integer::compareTo).orElse(strLength);
                if (max > strLength) {
                    Integer integer = indexList.indexOf(max);
                    XFunction(integer, indexList, strLength, max);
                }
                int loopTime = indexList.size() / 2;
                int index = 0;
                for (int i = 0; i < loopTime; i++) {
                    Integer first = indexList.get(index);
                    Integer next = indexList.get(index+1);
                    index += 2;
                    String bitwiseValue;
                    try{
                         bitwiseValue = stringValue.substring(first, next);
                    }catch(Exception e){ // 出现异常,值就为空
                        bitwiseValue = "";
                    }
                    splitFieldValues[i] = bitwiseValue;
                }
            }
        }
        return splitFieldValues;
    }

    @Override
    protected Column[] getOutputColumns() {
        List<PhysicalColumn> collect =
                Arrays.stream(splitTransformConfig.getOutputFields())
                        .map(
                                fieldName -> {
                                    return PhysicalColumn.of(
                                            fieldName, BasicType.STRING_TYPE, 200, true, "", "");
                                })
                        .collect(Collectors.toList());
        return collect.toArray(new Column[0]);
    }


    private List<Integer> parseSeparator(String separator) {
        String regex = "(\\d+)";
        Pattern compile = Pattern.compile(regex);
        Matcher matcher = compile.matcher(separator);
        List<Integer> indexList = Lists.newLinkedList();
        int matcher_start = 0;
        while (matcher.find(matcher_start)) {
            indexList.add(Integer.parseInt(matcher.group(1)));
            matcher_start = matcher.end();
        }
        return indexList;
    }

    private static void XFunction(Integer integer, List<Integer> indexList, Integer strLength, Integer max) {
        if (integer != -1) {
            indexList.set(integer, strLength);
            integer = indexList.indexOf(max);
            XFunction(integer, indexList, strLength, max);
        }
    }
}
