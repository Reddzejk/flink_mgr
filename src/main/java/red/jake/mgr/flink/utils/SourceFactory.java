package red.jake.mgr.flink.utils;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import red.jake.mgr.flink.model.RowAirline;
import red.jake.mgr.flink.model.RowCarrier;

import java.lang.reflect.Field;
import java.util.Arrays;

import static red.jake.mgr.flink.utils.SourceMetadata.*;

public class SourceFactory<T> {
    private SourceFactory() {
    }

    public static DataSet<RowAirline> getAirlineTypedSource(ExecutionEnvironment env, EnvironmentType envType) {
        String path = envType == EnvironmentType.LOCAL ? SAMPLE_AIRLINE.getPath() : AIRLINE.getPath();
        return new SourceFactory<RowAirline>().getTypedSource(env, path, RowAirline.class);
    }

    public static DataSet<RowCarrier> getCarrierTypedSource(ExecutionEnvironment env) {
        return new SourceFactory<RowCarrier>().getTypedSource(env, CARRIER.getPath(), RowCarrier.class);
    }

    private DataSource<T> getTypedSource(ExecutionEnvironment env, String path, Class<T> c) {
        Field[] fields = c.getFields();
        String[] names = Arrays.stream(fields).map(Field::getName).toArray(n -> new String[fields.length]);
        return env.readCsvFile(path)
                .ignoreFirstLine()
                .pojoType(c, names);
    }

    private DataSource<T> getT2ypedSource(ExecutionEnvironment env, String path, Class<T> c) {
        Field[] fields = c.getFields();
        String[] names = Arrays.stream(fields).map(Field::getName).toArray(n -> new String[fields.length]);
        return env.readCsvFile(path)
                .ignoreFirstLine()
                .pojoType(c, names);
    }
}
