package red.jake.mgr.flink.dataset;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.GroupReduceOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterators;
import red.jake.mgr.flink.BaseJob;
import red.jake.mgr.flink.model.AirlineHeader;
import red.jake.mgr.flink.model.RowAirline;
import red.jake.mgr.flink.model.RowYearCount;
import red.jake.mgr.flink.utils.EnvironmentType;
import red.jake.mgr.flink.utils.SourceFactory;

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
        GroupReduceOperator<RowAirline, RowYearCount> countedByYear = airlines
                .groupBy(AirlineHeader.year)
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
