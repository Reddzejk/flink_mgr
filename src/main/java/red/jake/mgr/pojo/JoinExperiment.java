package red.jake.mgr.pojo;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.utils.ParameterTool;
import red.jake.mgr.BaseJob;
import red.jake.mgr.pojo.model.AirlineHeader;
import red.jake.mgr.pojo.model.CarrierHeader;
import red.jake.mgr.pojo.model.RowAirline;
import red.jake.mgr.pojo.model.RowCarrier;
import red.jake.mgr.utils.EnvironmentType;
import red.jake.mgr.utils.SourceFactory;

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
