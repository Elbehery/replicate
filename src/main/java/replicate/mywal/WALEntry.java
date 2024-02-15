package replicate.mywal;

import java.nio.ByteBuffer;

public class WALEntry {
    public final static int sizeOfInt = 4, sizeOfLong = 8;
    private long timestamp, generation, entryIdx;
    private EntryType entryType;
    private byte[] data;

    public WALEntry(long generation, long entryIdx, EntryType entryType, byte[] data) {
        this.generation = generation;
        this.entryIdx = entryIdx;
        this.entryType = entryType;
        this.timestamp = System.currentTimeMillis();
        this.data = data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getGeneration() {
        return generation;
    }

    public long getEntryIdx() {
        return entryIdx;
    }

    public EntryType getEntryType() {
        return entryType;
    }

    public byte[] getData() {
        return data;
    }

    public ByteBuffer serialize() {
        var buf = ByteBuffer.allocate(sizeOfEntry());
        buf.clear();
        buf.putInt(serializedSize());
        buf.putInt(entryType.getVal());
        buf.putLong(generation);
        buf.putLong(entryIdx);
        buf.putLong(timestamp);
        buf.put(data);
        return buf;
    }

    // helpers
    private int sizeOfEntry() {
        return sizeOfInt + serializedSize();
    }

    private int serializedSize() {
        return sizeOfEntryIdx() + sizeOfGeneration() + sizeOfTimestamp() + sizeOfEntryType() + sizeOfData();
    }

    private int sizeOfGeneration() {
        return sizeOfLong;
    }

    private int sizeOfTimestamp() {
        return sizeOfLong;
    }

    private int sizeOfEntryIdx() {
        return sizeOfLong;
    }

    private int sizeOfData() {
        return data.length;
    }

    private int sizeOfEntryType() {
        return sizeOfInt;
    }
}
