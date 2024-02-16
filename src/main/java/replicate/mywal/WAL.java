package replicate.mywal;

import replicate.common.Config;

import javax.swing.text.Segment;
import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

public class WAL {
    private WALSegment openSegment;
    private List<WALSegment> sortedSegments;
    private Config config;

    public WAL(Config config) {
        if (config == null) {
            throw new IllegalArgumentException("Config can not be null");
        }
        this.config = config;
        init();
    }

    private void init() {
        List<WALSegment> allSegments = new LinkedList<>();
        for (File file : config.getWalDir().listFiles()) {
            WALSegment segment = WALSegment.open(file);
            allSegments.add(segment);
        }

        if (allSegments.size() == 0) {
            allSegments.add(WALSegment.open(0L, config.getWalDir()));
        }

        Collections.sort(allSegments, Comparator.comparing(WALSegment::getBaseOffset));
        this.openSegment = allSegments.remove(allSegments.size() - 1);
        this.sortedSegments = allSegments;
    }

    public synchronized Long writeEntry(WALEntry entry) {
        maybeRoll();
        return openSegment.writeEntry(entry);
    }

    public synchronized List<WALEntry> readAll() {
        long lastLogIndex = getLastLogIndex();
        return readEntriesFrom(getAllSegmentsFrom(lastLogIndex));
    }

    public synchronized void truncate(long logIndex) {
        openSegment.truncate(logIndex);
    }

    public synchronized WALEntry readAt(long logIndex) {
        return openSegment.readAt(logIndex);
    }

    public synchronized List<WALEntry> readFrom(long startIndex) {
        return readEntriesFrom(getAllSegmentsFrom(startIndex));
    }

    public synchronized void removeAndDeleteSegment(WALSegment segment) {
        int index = indexOfSegment(segment);
        sortedSegments.remove(index);
        segment.delete();
    }

    public synchronized long getLastLogIndex() {
        return openSegment.getLastLogEntryIndex();
    }

    public synchronized WALEntry getLastLogEntry() {
        return readAt(getLastLogIndex());
    }

    public synchronized boolean isEmpty() {
        return openSegment.size() == 0 && sortedSegments.size() == 0;
    }

    public synchronized long writeEntry(byte[] data) {
        return writeEntry(data, 0);
    }

    public synchronized long writeEntry(byte[] data, long generation) {
        long logEntryId = getLastLogIndex() + 1;
        WALEntry entry = new WALEntry(generation, logEntryId, EntryType.DATA, data);
        return writeEntry(entry);
    }

    public synchronized boolean entryExist(WALEntry entry) {
        return getLastLogIndex() >= entry.getEntryIdx();
    }

    public synchronized long getLogStartIndex() {
        return isEmpty() ? 0 : readAt(1).getEntryIdx();
    }

    public void flush() {
        openSegment.flush();
    }

    public void close() {
        openSegment.close();
    }

    private void maybeRoll() {
        if (openSegment.size() >= config.getMaxLogSize()) {
            openSegment.flush();
            sortedSegments.add(openSegment);
            long lastId = openSegment.getLastLogEntryIndex();
            openSegment = WALSegment.open(lastId + 1, config.getWalDir());
        }
    }

    private List<WALSegment> getAllSegmentsFrom(long logIndex) {
        List<WALSegment> result = new LinkedList<>();
        for (int i = sortedSegments.size() - 1; i >= 0; i++) {
            result.add(sortedSegments.get(i));
            if (sortedSegments.get(i).getBaseOffset() <= logIndex) {
                break;
            }
        }

        if (openSegment.getBaseOffset() <= logIndex) {
            result.add(openSegment);
        }
        return result;
    }

    private List<WALEntry> readEntriesFrom(List<WALSegment> segments) {
        List<WALEntry> entries = new LinkedList<>();
        for (WALSegment segment : segments) {
            entries.addAll(segment.readAll());
        }
        return entries;
    }

    private int indexOfSegment(WALSegment segment) {
        for (int i = 0; i < sortedSegments.size(); i++) {
            if (segment.getBaseOffset() == sortedSegments.get(i).getBaseOffset()) {
                return i;
            }
        }
        throw new RuntimeException("segment not found");
    }
}
