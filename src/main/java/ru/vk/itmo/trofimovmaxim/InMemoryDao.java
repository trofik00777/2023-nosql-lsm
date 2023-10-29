package ru.vk.itmo.trofimovmaxim;

import ru.vk.itmo.BaseEntry;
import ru.vk.itmo.Config;
import ru.vk.itmo.Dao;
import ru.vk.itmo.Entry;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InMemoryDao implements Dao<MemorySegment, Entry<MemorySegment>> {
    private static final String FILENAME = "sstable";

    private final ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTable;
    private final Config config;
    private final SsTablesList ssTables;

    private static final Comparator<MemorySegment> COMPARE_SEGMENT = (o1, o2) -> {
        if (o1 == null || o2 == null) {
            return o1 == null ? -1 : 1;
        }

        long mism = o1.mismatch(o2);
        if (mism == -1) {
            return (int) (o1.byteSize() - o2.byteSize());
        }
        if (mism == o1.byteSize() || mism == o2.byteSize()) {
            return mism == o1.byteSize() ? -1 : 1;
        }
        return Byte.compare(
                o1.get(ValueLayout.OfByte.JAVA_BYTE, mism),
                o2.get(ValueLayout.OfByte.JAVA_BYTE, mism)
        );
    };

    public InMemoryDao() {
        memTable = new ConcurrentSkipListMap<>(COMPARE_SEGMENT);
        config = null;
        ssTables = null;
    }

    public InMemoryDao(Config config) {
//        this.config = config;
//        memTable = new ConcurrentSkipListMap<>(COMPARE_SEGMENT);
//        SsTable sstable = new SsTable(config.basePath(), FILENAME);
//        if (sstable.data == null || sstable.offsetsTable == null) {
//            ssTable = null;
//        } else {
//            ssTable = sstable;
//        }


        this.config = config;
        memTable = new ConcurrentSkipListMap<>(COMPARE_SEGMENT);

        SsTablesList sstables = new SsTablesList();
        if (sstables.ssTables == null) {
            ssTables = null;
        } else {
            ssTables = sstables;
        }
    }

    @Override
    public Iterator<Entry<MemorySegment>> get(MemorySegment from, MemorySegment to) {
        return new DaoIter(ssTables, from, to);
    }

    public ConcurrentNavigableMap<MemorySegment, Entry<MemorySegment>> memTableFromTo(MemorySegment from, MemorySegment to) {
        if (from == null || to == null) {
            if (from == null && to == null) {
                return memTable;
            } else if (from == null) {
                return memTable.headMap(to);
            } else {
                return memTable.tailMap(from);
            }
        } else {
            return memTable.subMap(from, to);
        }
    }

    @Override
    public Entry<MemorySegment> get(MemorySegment key) {
        var resultMemTable = memTable.get(key);
        if (resultMemTable != null) {
            return resultMemTable;
        }
        if (this.ssTables != null) {
            return this.ssTables.get(key);
        }
        return null;
    }

    @Override
    public void upsert(Entry<MemorySegment> entry) {
        memTable.put(entry.key(), entry);
    }

    private long getNewFilenameIndex() throws IOException {
        try (var walk = Files.walk(config.basePath())) {
            return walk.filter(Files::isRegularFile)
                    .map(path -> {
                        String name = path.getFileName().toString();
                        return Long.parseLong(name.substring(FILENAME.length() + 1, name.length() - 5));
                    }).max(Comparator.naturalOrder()).orElse(-1L) + 1L;
        }
    }

    @Override
    public void flush() throws IOException {
        if (config == null || ssTables == null) {
            return;
        }
        ssTables.ssTables.forEach(ssTable -> ssTable.arena.close());

        long size = 0;
        for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : memTable.entrySet()) {
            size += entry.getKey().byteSize() + entry.getValue().key().byteSize() + entry.getValue().value().byteSize();
        }

        try (Arena writeArena = Arena.ofConfined()) {
            String filenameCurrent = String.format("%s_%d", FILENAME, getNewFilenameIndex());
            try (FileChannel fileChannelData = FileChannel.open(
                    config.basePath().resolve(filenameCurrent + ".data"),
                    StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.CREATE);
                 FileChannel fileChannelMeta = FileChannel.open(
                         config.basePath().resolve(filenameCurrent + ".meta"),
                         StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING,
                         StandardOpenOption.CREATE)) {
                MemorySegment pageData = fileChannelData.map(FileChannel.MapMode.READ_WRITE, 0, size, writeArena);
                MemorySegment pageMeta = fileChannelMeta.map(FileChannel.MapMode.READ_WRITE, 0,
                        4L * memTable.size() * Long.BYTES, writeArena);

                long offsetData = 0;
                long offsetMeta = 0;

                for (Map.Entry<MemorySegment, Entry<MemorySegment>> entry : memTable.entrySet()) {
                    MemorySegment key = entry.getKey();

                    pageMeta.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetMeta, offsetData);
                    offsetMeta += Long.BYTES;

                    long keySize = key.byteSize();
                    pageMeta.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetMeta, keySize);
                    offsetMeta += Long.BYTES;

                    Entry<MemorySegment> val = entry.getValue();
                    long val1Size = val.key().byteSize();
                    pageMeta.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetMeta, val1Size);
                    offsetMeta += Long.BYTES;

                    long val2Size = val.value().byteSize();
                    pageMeta.set(ValueLayout.JAVA_LONG_UNALIGNED, offsetMeta, val2Size);
                    offsetMeta += Long.BYTES;

                    MemorySegment.copy(key, 0, pageData, offsetData, keySize);
                    offsetData += key.byteSize();
                    MemorySegment.copy(val.key(), 0, pageData, offsetData, val1Size);
                    offsetData += val.key().byteSize();
                    MemorySegment.copy(val.value(), 0, pageData, offsetData, val2Size);
                    offsetData += val.value().byteSize();
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        flush();
    }

    private class SsTablesList {
        private List<SsTable> ssTables;

        SsTablesList() {
            boolean created = false;
            try {
                ssTables = new ArrayList<>();
                created = true;

                for (long i = 0; i < getNewFilenameIndex(); ++i) {
                    var ssTable = new SsTable(config.basePath(), String.format("%s_%d", FILENAME, i));

                    if (ssTable.data == null || ssTable.offsetsTable == null) {
                        created = false;
                        break;
                    }
                    ssTables.add(ssTable);
                }
            } catch (Exception e) {
                created = false;
            } finally {
                if (!created) {
                    ssTables = null;
                }
            }
        }

        public Entry<MemorySegment> get(MemorySegment key) {
            for (int i = ssTables.size() - 1; i >= 0; --i) {
                var result = ssTables.get(i).get(key);
                if (result != null) {
                    return result;
                }
            }
            return null;
        }
    }

    private static class SsTable {
        private MemorySegment data;
        private MemorySegment offsetsTable;
        private long size;
        private final Arena arena;

        SsTable(Path basePath, String filename) {
            Path pathToSsTable = basePath.resolve(filename + ".data");
            Path pathToOffsetsTable = basePath.resolve(filename + ".meta");

            arena = Arena.ofShared();

            Logger logger = Logger.getLogger(InMemoryDao.class.getName());
            try (FileChannel channelData = FileChannel.open(pathToSsTable, StandardOpenOption.READ);
                 FileChannel channelMeta = FileChannel.open(pathToOffsetsTable, StandardOpenOption.READ)) {
                long sizeData = Files.size(pathToSsTable);
                long sizeMeta = Files.size(pathToOffsetsTable);

                this.size = (sizeData / Long.BYTES / 4L);

                data = channelData.map(FileChannel.MapMode.READ_ONLY, 0, sizeData, arena);
                offsetsTable = channelMeta.map(FileChannel.MapMode.READ_ONLY, 0, sizeMeta, arena);
            } catch (FileNotFoundException e) {
                logger.log(Level.WARNING, String.format("File not found: %s", e));
            } catch (IOException e) {
                logger.log(Level.WARNING, String.format("Some IOException: %s", e));
            } catch (Exception e) {
                logger.log(Level.WARNING, String.format("Some exception: %s", e));
            }

            if (data == null || offsetsTable == null) {
                arena.close();
            }
        }

        Offset getOffset(long index) {
            long offset = 4L * index * Long.BYTES;
            long keyOffset = offsetsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset);
            long keySize = offsetsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + Long.BYTES);
            long val1Size = offsetsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + 2L * Long.BYTES);
            long val2Size = offsetsTable.get(ValueLayout.JAVA_LONG_UNALIGNED, offset + 3L * Long.BYTES);

            return new Offset(keyOffset, keySize, val1Size, val2Size);
        }

        Entry<MemorySegment> get(MemorySegment key) {
            long l = -1;
            long r = size;
            while (l < r - 1) {
                long m = (l + r) / 2;
                var offset = getOffset(m);
                MemorySegment dataM = data.asSlice(offset.keyOffset, offset.keySize);
                int cmp = COMPARE_SEGMENT.compare(dataM, key);
                if (cmp == 0) {
                    return new BaseEntry<>(
                            data.asSlice(offset.keyOffset + offset.keySize, offset.val1Size),
                            data.asSlice(offset.keyOffset + offset.keySize + offset.val1Size, offset.val2Size)
                    );
                }
                if (cmp < 0) {
                    l = m;
                } else {
                    r = m;
                }
            }

            return null;
        }

        long getLeft(MemorySegment key) {
            long l = -1;
            long r = size;
            while (l < r - 1) {
                long m = (l + r) / 2;
                var offset = getOffset(m);
                MemorySegment dataM = data.asSlice(offset.keyOffset, offset.keySize);
                int cmp = COMPARE_SEGMENT.compare(dataM, key);
                if (cmp < 0) {
                    l = m;
                } else {
                    r = m;
                }
            }

            return r;
        }

        long getRight(MemorySegment key) {
            long l = -1;
            long r = size;
            while (l < r - 1) {
                long m = (l + r) / 2;
                var offset = getOffset(m);
                MemorySegment dataM = data.asSlice(offset.keyOffset, offset.keySize);
                int cmp = COMPARE_SEGMENT.compare(dataM, key);
                if (cmp <= 0) {
                    l = m;
                } else {
                    r = m;
                }
            }

            return r;
        }

        static class Offset implements Serializable {
            private final long keyOffset;
            private final long keySize;
            private final long val1Size;
            private final long val2Size;

            public Offset(long keyOffset, long keySize, long val1Size, long val2Size) {
                this.keyOffset = keyOffset;
                this.keySize = keySize;
                this.val1Size = val1Size;
                this.val2Size = val2Size;
            }
        }
    }

    private static class SsTableIter implements Iterator<Entry<MemorySegment>> {
        SsTable data;
        long currentIndex = 0;
        long to;

        SsTableIter(SsTable sstable, MemorySegment from, MemorySegment to) {
            data = sstable;
            if (from != null) {
                currentIndex = data.getLeft(from);
            }
            if (to == null) {
                this.to = data.size - 1;
            } else {
                this.to = data.getRight(to);
            }
        }

        @Override
        public boolean hasNext() {
            return currentIndex <= to;
        }

        @Override
        public Entry<MemorySegment> next() {
            var offset = data.getOffset(currentIndex);
            currentIndex++;
            return new BaseEntry<>(
                    data.data.asSlice(offset.keyOffset + offset.keySize, offset.val1Size),
                    data.data.asSlice(offset.keyOffset + offset.keySize + offset.val1Size, offset.val2Size)
            );
        }

        public MemorySegment nextKey() {
            var offset = data.getOffset(currentIndex);
            return data.data.asSlice(offset.keyOffset, offset.keySize);
        }
    }

    private class MemTableIter implements Iterator<Entry<MemorySegment>> {
        private final Iterator<Map.Entry<MemorySegment, Entry<MemorySegment>>> iter;
        Map.Entry<MemorySegment, Entry<MemorySegment>> current;

        MemTableIter(MemorySegment from, MemorySegment to) {
            this.iter = memTableFromTo(from, to).entrySet().iterator();
            if (iter.hasNext()) {
                this.current = iter.next();
            }
        }

        @Override
        public boolean hasNext() {
            return current != null;
        }

        @Override
        public Entry<MemorySegment> next() {
            var result = current;
            if (iter.hasNext()) {
                this.current = iter.next();
            } else {
                this.current = null;
            }
            return result.getValue();
        }

        public MemorySegment nextKey() {
            return current.getKey();
        }
    }

    private class DaoIter implements Iterator<Entry<MemorySegment>> {
        private MemTableIter memTableIter;
        private TreeSet<MemSegmentWithIndex> queue;
        private List<SsTableIter> iters;

        DaoIter(SsTablesList ssTables, MemorySegment from, MemorySegment to) {
            this.memTableIter = new MemTableIter(from, to);
            queue = new TreeSet<>();

            if (ssTables != null) {
                iters = new ArrayList<>();
                for (SsTable ssTable : ssTables.ssTables) {
                    iters.add(new SsTableIter(ssTable, from, to));
                }

                for (int i = 0; i < iters.size(); ++i) {
                    if (iters.get(i).hasNext()) {
                        queue.add(new MemSegmentWithIndex(iters.get(i).nextKey(), i));
                    }
                }
            }
        }

        @Override
        public boolean hasNext() {
            return memTableIter.hasNext() || !queue.isEmpty();
        }

        @Override
        public Entry<MemorySegment> next() {
            if (queue.isEmpty()) {
                return memTableIter.next();
            }
            var fromSstable = queue.first();
            var fromMemTable = memTableIter.nextKey();
            int cmp = COMPARE_SEGMENT.compare(fromMemTable, fromSstable.data);
            if (cmp <= 0) {
                return memTableIter.next();
            }
            var result = iters.get(fromSstable.index).next();
            queue.remove(fromSstable);
            if (iters.get(fromSstable.index).hasNext()) {
                queue.add(new MemSegmentWithIndex(iters.get(fromSstable.index).nextKey(), fromSstable.index));
            }
            return result;
        }

        private class MemSegmentWithIndex implements Comparable<MemSegmentWithIndex> {
            private MemorySegment data;
            private int index;

            public MemSegmentWithIndex(MemorySegment data, int index) {
                this.data = data;
                this.index = index;
            }

            @Override
            public int compareTo(MemSegmentWithIndex other) {
                int cmp = COMPARE_SEGMENT.compare(data, other.data);
                if (cmp == 0) {
                    return index < other.index ? -1 : 1;
                }
                return cmp;
            }
        }
    }
}
