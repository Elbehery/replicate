package replicate.mywal;

import java.nio.ByteBuffer;

public class WALEntry {
    private static int sizeOfInt = 4;
    private static int sizeOfLong = 8;
    private long entryIndex;
    private long generation;
    private long timestamp;
    private EntryType entryType;
    private byte[] data;

    public WALEntry(long entryIndex, long generation, EntryType entryType, byte[] data) {
        this.entryIndex = entryIndex;
        this.generation = generation;
        this.timestamp = System.currentTimeMillis();
        this.entryType = entryType;
        this.data = data;
    }

    public long getEntryIndex() {
        return entryIndex;
    }

    public long getGeneration() {
        return generation;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public EntryType getEntryType() {
        return entryType;
    }

    public byte[] getData() {
        return data;
    }

    public ByteBuffer serialize() {
        var buf = ByteBuffer.allocate(sizeOfWALEntry());
        buf.clear();
        buf.putInt(serializedSize());
        buf.putInt(entryType.getVal());
        buf.putLong(generation);
        buf.putLong(entryIndex);
        buf.putLong(timestamp);
        buf.put(data);
        return buf;
    }

    // helpers
    private int sizeOfWALEntry() {
        return sizeOfInt + serializedSize();
    }

    private int serializedSize() {
        return sizeOfEntryIndex() + sizeOfEntryType() + sizeOfGeneration() + sizeOfTimestamp() + sizeOfData();
    }

    private int sizeOfData() {
        return data.length;
    }

    private int sizeOfEntryIndex() {
        return sizeOfLong;
    }

    private int sizeOfGeneration() {
        return sizeOfLong;
    }

    private int sizeOfTimestamp() {
        return sizeOfLong;
    }

    private int sizeOfEntryType() {
        return sizeOfInt;
    }
}
