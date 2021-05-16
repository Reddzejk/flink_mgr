package red.jake.mgr.pojo.model;

import java.io.Serializable;

public class RowDelayType implements Serializable {
    public String flightNum;
    public String delayType;
    public int delayTime;

    public RowDelayType() {
    }

    public RowDelayType(String flightNum, String delayType, int delayTime) {
        this.flightNum = flightNum;
        this.delayType = delayType;
        this.delayTime = delayTime;
    }

    @Override
    public String toString() {
        return String.format("flightNum: %s, delayType: %s, delayTime: %s | ", flightNum, delayType, delayTime);
    }
}
