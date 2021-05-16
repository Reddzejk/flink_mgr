package red.jake.mgr.utils;

public enum SourceMetadata {
    AIRLINE("file:///home/red/mgr/data/airline.csv"),
    CARRIER("file:///home/red/mgr/data/carrier.csv"),
    SAMPLE_AIRLINE("file:///home/red/mgr/data/1000sample.csv");

    private final String path;

    SourceMetadata(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }
}
