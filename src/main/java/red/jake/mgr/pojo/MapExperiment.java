package red.jake.mgr.pojo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import red.jake.mgr.BaseJob;
import red.jake.mgr.pojo.model.RowAirline;
import red.jake.mgr.pojo.model.RowAirlineDated;
import red.jake.mgr.utils.EnvironmentType;
import red.jake.mgr.utils.SourceFactory;

import java.time.LocalDate;

public class MapExperiment extends BaseJob {
    public MapExperiment(ParameterTool params) {
        super(params);
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        new MapExperiment(params).runJob();
    }

    public void runJob() throws Exception {
        DataSet<RowAirline> airlines = SourceFactory.getAirlineTypedSource(env, EnvironmentType.valueOf(envType));
        DataSet<RowAirlineDated> mapped = airlines.map(new MapFunction<RowAirline, RowAirlineDated>() {
            @Override
            public RowAirlineDated map(RowAirline rowAirline) throws Exception {
                RowAirlineDated dated = new RowAirlineDated();
                dated.date = LocalDate.parse(rowAirline.year + "-" + unifyString(rowAirline.month) + "-" + unifyString(rowAirline.dayOfMonth));
                dated.actualElapsedTime = rowAirline.actualElapsedTime;
                dated.actualElapsedTime = rowAirline.actualElapsedTime;
                dated.airTime = rowAirline.airTime;
                dated.arrDelay = rowAirline.arrDelay;
                dated.arrTime = rowAirline.arrTime;
                dated.crsArrTime = rowAirline.crsArrTime;
                dated.crsDepTime = rowAirline.crsDepTime;
                dated.crsElapsedTime = rowAirline.crsElapsedTime;
                dated.cancellationCode = rowAirline.cancellationCode;
                dated.cancelled = rowAirline.cancelled;
                dated.carrierDelay = rowAirline.carrierDelay;
                dated.dayOfWeek = rowAirline.dayOfWeek;
                dated.depDelay = rowAirline.depDelay;
                dated.depTime = rowAirline.depTime;
                dated.dest = rowAirline.dest;
                dated.distance = rowAirline.distance;
                dated.diverted = rowAirline.diverted;
                dated.flightNum = rowAirline.flightNum;
                dated.lateAircraftDelay = rowAirline.lateAircraftDelay;
                dated.nasDelay = rowAirline.nasDelay;
                dated.origin = rowAirline.origin;
                dated.securityDelay = rowAirline.securityDelay;
                dated.tailNum = rowAirline.tailNum;
                dated.taxiIn = rowAirline.taxiIn;
                dated.taxiOut = rowAirline.taxiOut;
                dated.uniqueCarrier = rowAirline.uniqueCarrier;
                dated.weatherDelay = rowAirline.weatherDelay;
                return dated;
            }

            private String unifyString(String unit) {
                return unit.length() < 2 ? "0" + unit : unit;
            }
        });

        mapped.print();
    }

}
