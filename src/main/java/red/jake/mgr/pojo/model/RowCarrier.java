package red.jake.mgr.pojo.model;

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
