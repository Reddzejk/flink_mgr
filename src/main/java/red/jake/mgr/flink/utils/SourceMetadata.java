package red.jake.mgr.flink.utils;

public enum SourceMetadata {
    AIRLINE("E:\\Praca Magisterska\\project_mgr\\data\\airline.csv"),
    CARRIER("E:\\Praca Magisterska\\project_mgr\\data\\carrier.csv"),
    SAMPLE_AIRLINE("E:\\Praca Magisterska\\project_mgr\\data\\1000sample.csv");

    private final String path;

    SourceMetadata(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }
}
