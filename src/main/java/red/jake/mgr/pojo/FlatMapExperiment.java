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

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import red.jake.mgr.BaseJob;
import red.jake.mgr.pojo.model.AirlineHeader;
import red.jake.mgr.pojo.model.RowAirline;
import red.jake.mgr.pojo.model.RowDelayType;
import red.jake.mgr.utils.EnvironmentType;
import red.jake.mgr.utils.SourceFactory;

import java.util.Optional;

public class FlatMapExperiment extends BaseJob {

    public FlatMapExperiment(ParameterTool params) {
        super(params);
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        new FlatMapExperiment(params).runJob();
    }

    public void runJob() throws Exception {
        DataSet<RowAirline> airlines = SourceFactory.getAirlineTypedSource(env, EnvironmentType.valueOf(envType));
        DataSet<RowDelayType> delays = airlines.flatMap(new FlatMapFunction<RowAirline, RowDelayType>() {
            @Override
            public void flatMap(RowAirline rowAirline, Collector<RowDelayType> collector) {
                computeDelay(rowAirline.flightNum, AirlineHeader.arrDelay, rowAirline.arrDelay).ifPresent(collector::collect);
                computeDelay(rowAirline.flightNum, AirlineHeader.depDelay, rowAirline.depDelay).ifPresent(collector::collect);
                computeDelay(rowAirline.flightNum, AirlineHeader.nasDelay, rowAirline.nasDelay).ifPresent(collector::collect);
                computeDelay(rowAirline.flightNum, AirlineHeader.carrierDelay, rowAirline.carrierDelay).ifPresent(collector::collect);
                computeDelay(rowAirline.flightNum, AirlineHeader.weatherDelay, rowAirline.weatherDelay).ifPresent(collector::collect);
                computeDelay(rowAirline.flightNum, AirlineHeader.lateAircraftDelay, rowAirline.lateAircraftDelay).ifPresent(collector::collect);
                computeDelay(rowAirline.flightNum, AirlineHeader.securityDelay, rowAirline.securityDelay).ifPresent(collector::collect);
            }

            private Optional<RowDelayType> computeDelay(String flightNum, String delayType, String delayStr) {
                int delay = getDelay(delayStr);
                return delay > 0 ? Optional.of(new RowDelayType(flightNum, delayType, delay)) : Optional.empty();
            }

            private int getDelay(String delay) {
                boolean isNumber = NumberUtils.isNumber(delay);
                return isNumber ? Integer.parseInt(delay) : 0;
            }
        });
        delays.print();
    }


}
