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
