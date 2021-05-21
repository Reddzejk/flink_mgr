package red.jake.mgr.flink.dataset;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import red.jake.mgr.flink.BaseJob;
import red.jake.mgr.flink.model.AirlineHeader;
import red.jake.mgr.flink.model.CarrierHeader;
import red.jake.mgr.flink.model.RowAirline;
import red.jake.mgr.flink.model.RowCarrier;
import red.jake.mgr.flink.utils.EnvironmentType;
import red.jake.mgr.flink.utils.SourceFactory;

public class JoinExperiment extends BaseJob {

    public JoinExperiment(ParameterTool params) {
        super(params);
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        new JoinExperiment(params).runJob();
    }

    public void runJob() throws Exception {
        DataSet<RowAirline> airlines = SourceFactory.getAirlineTypedSource(env, EnvironmentType.valueOf(envType));
        DataSet<RowCarrier> carriers = SourceFactory.getCarrierTypedSource(env);
        JoinOperator.DefaultJoin<RowAirline, RowCarrier> joined = airlines.joinWithTiny(carriers)
                .where(AirlineHeader.uniqueCarrier)
                .equalTo(CarrierHeader.code);
        joined.print();
    }
}
