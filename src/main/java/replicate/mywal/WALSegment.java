package replicate.mywal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WALSegment {
    private static final String logPrefix = "wal", logSuffix = ".log";
    final FileChannel fileChannel;
    final RandomAccessFile randomAccessFile;
    private File file;
    private Map<Long, Long> entriesOffsetMap;

    private WALSegment(long startIdx, File file) {
        try {
            this.file = file;
            this.randomAccessFile = new RandomAccessFile(file, "rw");
            this.fileChannel = randomAccessFile.getChannel();
            buildEntriesOffsetMap();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized String getFileName() {
        return file.getPath();
    }

    public synchronized Long getBaseOffset() {
        return getBaseOffsetFromFileName(file.getName());
    }

    public synchronized List<WALEntry> readFrom(Long startIndex) {
        var deserializer = new WALEntryDeserializer(fileChannel);
        var result = new LinkedList<WALEntry>();
        List<Long> indexes = entriesOffsetMap.keySet().stream().filter(idx -> idx >= startIndex).collect(Collectors.toList());

        for (Long idx : indexes) {
            var walEntry = deserializer.readEntry(entriesOffsetMap.get(idx));
            result.add(walEntry);
        }
        return result;
    }

    public synchronized List<WALEntry> readAll() {
        return readFrom(getFirstLogEntryIndex());
    }

    public synchronized WALEntry readAt(Long index) {
        var fileOffset = entriesOffsetMap.get(index);
        var deserializer = new WALEntryDeserializer(fileChannel);
        return deserializer.readEntry(fileOffset);
    }

    public synchronized void truncate(Long index) {
        var offset = entriesOffsetMap.get(index);
        try {
            fileChannel.truncate(offset);
            entriesOffsetMap.keySet().removeIf(key -> key >= index);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized Long getFirstLogEntryIndex() {
        return entriesOffsetMap.keySet().stream().min(Long::compareTo).get();
    }

    public synchronized Long getFirstLogEntryTimestamp() {
        var entry = readAt(getFirstLogEntryIndex());
        return entry.getTimestamp();
    }

    public synchronized Long getLastLogEntryIndex() {
        return entriesOffsetMap.keySet().stream().max(Long::compareTo).get();
    }

    public synchronized Long getLastLogEntryTimestamp() {
        var entry = readAt(getLastLogEntryIndex());
        return entry.getTimestamp();
    }

    public synchronized Long writeEntry(WALEntry walEntry) {
        try {
            var idx = walEntry.getEntryIdx();
            var offset = fileChannel.size();
            writeToChannel(walEntry.serialize());
            entriesOffsetMap.put(idx, offset);
            return idx;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public synchronized void delete() {
        try {
            fileChannel.close();
            randomAccessFile.close();
            Files.deleteIfExists(file.toPath());
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    public synchronized long size() {
        try {
            return fileChannel.size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void flush() {
        try {
            fileChannel.force(true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void close() {
        flush();

        try {
            fileChannel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Long getBaseOffsetFromFileName(String fileName) {
        String[] splits = fileName.split(logSuffix)[0].split("_");
        if (splits[0].equals(logPrefix)) {
            return Long.parseLong(splits[1]);
        }
        return -1L;
    }

    public static String createFileName(Long startIndex) {
        return logPrefix + "_" + startIndex + logSuffix;
    }

    public static WALSegment open(Long startIndex, File walDir) {
        var file = new File(walDir, createFileName(startIndex));
        return new WALSegment(startIndex, file);
    }

    public static WALSegment open(File file) {
        return new WALSegment(getBaseOffsetFromFileName(file.getName()), file);
    }

    private synchronized void buildEntriesOffsetMap() {
        try {
            this.entriesOffsetMap = new HashMap<>();
            var deserializer = new WALEntryDeserializer(fileChannel);
            var bytesRead = 0L;
            while (bytesRead < fileChannel.size()) {
                var entry = deserializer.readEntry(bytesRead);
                entriesOffsetMap.put(entry.getEntryIdx(), bytesRead);
                bytesRead += entry.sizeOfEntry();
            }
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    private Long writeToChannel(ByteBuffer buf) {
        buf.flip();

        try {
            fileChannel.write(buf);
            while (buf.hasRemaining()) {
                fileChannel.write(buf);
            }
            flush();
            return fileChannel.position();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}