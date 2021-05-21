package red.jake.mgr.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import static red.jake.mgr.flink.utils.EnvironmentType.LOCAL;

public abstract class BaseJob {
    protected final String envType;
    protected final ExecutionEnvironment env;

    public BaseJob(ParameterTool params) {
        envType = params.get("envtype", LOCAL.name());
        env = ExecutionEnvironment.getExecutionEnvironment();
    }

}
