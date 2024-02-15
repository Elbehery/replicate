package replicate.mywal;

public enum EntryType {
    DATA(0),
    METADATA(1),
    CRC(2);

    private int val;

    EntryType(int val) {
        this.val = val;
    }

    public int getVal() {
        return val;
    }
}
