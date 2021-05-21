package red.jake.mgr.flink.model;

import java.io.Serializable;

public class RowCarrier implements Serializable {
    public String code;
    public String description;

    public RowCarrier() {
    }

    @Override
    public String toString() {
        return String.format("code: %s, description: %s | ", code, description);
    }
}
