package replicate.mywal;

import java.util.HashMap;
import java.util.Map;

public enum EntryType {
    DATA(0), METADATA(1), CRC(2);
    private int val;

    private static Map<Integer, EntryType> map = new HashMap<>();

    static {
        for (EntryType entryType : EntryType.values()) {
            map.put(entryType.val, entryType);
        }
    }

    public static EntryType valueOf(int entryType) {
        return map.get(entryType);
    }

    EntryType(int val) {
        this.val = val;
    }

    public int getVal() {
        return val;
    }
}
