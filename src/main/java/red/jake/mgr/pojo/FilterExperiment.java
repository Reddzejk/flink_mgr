package red.jake.mgr.pojo;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.utils.ParameterTool;
import red.jake.mgr.BaseJob;
import red.jake.mgr.pojo.model.RowAirline;
import red.jake.mgr.utils.EnvironmentType;
import red.jake.mgr.utils.SourceFactory;

public class FilterExperiment extends BaseJob {

    public FilterExperiment(ParameterTool params) {
        super(params);
    }

    public static void main(String[] args) throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);
        new FilterExperiment(params).runJob();
    }

    public void runJob() throws Exception {
        DataSet<RowAirline> airlines = SourceFactory.getAirlineTypedSource(env, EnvironmentType.valueOf(envType));
        DataSet<RowAirline> filtered = airlines.filter(rowAirline -> rowAirline.year.equals("2008"));
        filtered.print();
    }
}
