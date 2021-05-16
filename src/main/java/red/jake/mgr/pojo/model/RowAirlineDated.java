package red.jake.mgr.pojo.model;

import java.time.LocalDate;

public class RowAirlineDated {
    public String actualElapsedTime;
    public String airTime;
    public String arrDelay;
    public String arrTime;
    public String crsArrTime;
    public String crsDepTime;
    public String crsElapsedTime;
    public String cancellationCode;
    public String cancelled;
    public String carrierDelay;
    public String dayOfWeek;
    public String depDelay;
    public String depTime;
    public String dest;
    public String distance;
    public String diverted;
    public String flightNum;
    public String lateAircraftDelay;
    public String nasDelay;
    public String origin;
    public String securityDelay;
    public String tailNum;
    public String taxiIn;
    public String taxiOut;
    public String uniqueCarrier;
    public String weatherDelay;
    public LocalDate date;

    public RowAirlineDated() {
    }

    @Override
    public String toString() {
        return "actualElapsedTime:" + actualElapsedTime +
                ", airTime:" + airTime +
                ", arrDelay:" + arrDelay +
                ", arrTime:" + arrTime +
                ", crsArrTime:" + crsArrTime +
                ", crsDepTime:" + crsDepTime +
                ", crsElapsedTime:" + crsElapsedTime +
                ", cancellationCode:" + cancellationCode +
                ", cancelled:" + cancelled +
                ", carrierDelay:" + carrierDelay +
                ", dayOfWeek:" + dayOfWeek +
                ", depDelay:" + depDelay +
                ", depTime:" + depTime +
                ", dest:" + dest +
                ", distance:" + distance +
                ", diverted:" + diverted +
                ", flightNum:" + flightNum +
                ", lateAircraftDelay:" + lateAircraftDelay +
                ", nasDelay:" + nasDelay +
                ", origin:" + origin +
                ", securityDelay:" + securityDelay +
                ", tailNum:" + tailNum +
                ", taxiIn:" + taxiIn +
                ", taxiOut:" + taxiOut +
                ", uniqueCarrier:" + uniqueCarrier +
                ", weatherDelay:" + weatherDelay +
                ", date:" + date + " | ";
    }
}
