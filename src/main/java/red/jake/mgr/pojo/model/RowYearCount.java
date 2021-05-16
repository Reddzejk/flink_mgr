package red.jake.mgr.pojo.model;

import java.io.Serializable;

public class RowYearCount implements Serializable {
    public String year;
    public int count;

    public RowYearCount() {
    }

    @Override
    public String toString() {
        return String.format("year: %s, count: %s | ", year, count);
    }
}
