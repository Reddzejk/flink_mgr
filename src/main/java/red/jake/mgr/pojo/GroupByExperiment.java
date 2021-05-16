/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package red.jake.mgr.pojo;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;
import red.jake.mgr.BaseJob;
import red.jake.mgr.pojo.model.AirlineHeader;
import red.jake.mgr.pojo.model.RowAirline;
import red.jake.mgr.pojo.model.RowYearCount;
import red.jake.mgr.utils.EnvironmentType;
import red.jake.mgr.utils.SourceFactory;

import java.util.Iterator;

public class GroupByExperiment extends BaseJob {

    public GroupByExperiment(ParameterTool params) {
        super(params);
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        new GroupByExperiment(params).runJob();
    }

    public void runJob() throws Exception {
        DataSet<RowAirline> airlines = SourceFactory.getAirlineTypedSource(env, EnvironmentType.valueOf(envType));
        GroupReduceOperator<RowAirline, RowYearCount> countedByYear = airlines.groupBy(AirlineHeader.year)
                .reduceGroup((GroupReduceFunction<RowAirline, RowYearCount>) (iterable, collector) -> {
                    Iterator<RowAirline> iterator = iterable.iterator();
                    RowYearCount yearCount = new RowYearCount();
                    yearCount.year = iterator.next().year;
                    yearCount.count = Iterators.size(iterator) + 1;
                    collector.collect(yearCount);
                }).returns(RowYearCount.class);
        countedByYear.print();
    }
}
