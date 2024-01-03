/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class CalculateAverage_darimont {

    private static final String FILE = System.getProperty("file", "./measurements.txt");
    private static final long CHUNK_SIZE_IN_BYTES = Integer.getInteger("chunks_mb", 16) * 1024 * 1024;
    private static final int THREAD_COUNT = Integer.getInteger("threads", Runtime.getRuntime().availableProcessors());

    private record Statistics(double min, double max, double sum, long count) {

        Statistics(double value) {
            this(value, value, value, 1);
        }

        public static Statistics merge(Statistics s1, Statistics s2) {
            return new Statistics(Math.min(s1.min, s2.min), Math.max(s1.max, s2.max), s1.sum + s2.sum, s1.count + s2.count);
        }

        public String toString() {
            return round(min) + "/" + round(sum / count) + "/" + round(max);
        }

        private double round(double value) {
            return Math.round(value * 10.0) / 10.0;
        }
    }

    private static double parseTemperature(String s) {
        boolean negative = s.charAt(0) == '-';

        int length = s.length();
        double result = 0.0;
        int decimalPlaces = 0;
        boolean decimalFound = false;

        for (int i = negative ? 1 : 0; i < length; i++) {
            char c = s.charAt(i);
            if (c == '.') {
                decimalFound = true;
                continue;
            }
            if (decimalFound && decimalPlaces < 2) { // Limit to 2 decimal places
                decimalPlaces++;
                result = result * 10 + (c - '0');
            }
            else if (!decimalFound) {
                result = result * 10 + (c - '0');
            }
        }

        while (decimalPlaces < 2) { // Adjust for missing decimal places
            result *= 10;
            decimalPlaces++;
        }

        return negative ? -result / 100 : result / 100;
    }

    public static void main(String[] args) throws Exception {

        try (var executorService = Executors.newFixedThreadPool(THREAD_COUNT)) {
            var stats = new ConcurrentHashMap<String, Statistics>();
            try (var fileChannel = FileChannel.open(Path.of(FILE), StandardOpenOption.READ)) {
                var fileSize = fileChannel.size();
                var futures = new ArrayList<Future<?>>((int) (fileSize / CHUNK_SIZE_IN_BYTES) + 1);
                var currentChunkStartOffset = 0L;

                while (currentChunkStartOffset < fileSize) {
                    var chunkEndOffset = Math.min(currentChunkStartOffset + CHUNK_SIZE_IN_BYTES, fileSize);
                    chunkEndOffset = ensureChunkOffsetEndsWithCompleteLine(fileChannel, chunkEndOffset, fileSize);
                    futures.add(executorService.submit(createChunkProcessingTask(fileChannel, currentChunkStartOffset, chunkEndOffset, stats, fileSize)));
                    currentChunkStartOffset = chunkEndOffset;
                }

                // Wait for all tasks to complete
                for (Future<?> future : futures) {
                    future.get();
                }
            }

            var sortedStats = new TreeMap<String, String>();
            for (var entry : stats.entrySet()) {
                sortedStats.put(entry.getKey(), entry.getValue().toString());
            }
            System.out.println(sortedStats);
        }
    }

    private static Runnable createChunkProcessingTask(FileChannel fileChannel, long chunkStart, long chunkEnd, ConcurrentHashMap<String, Statistics> stats,
                                                      long fileSize) {
        return () -> {
            try {
                var mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, chunkStart, chunkEnd - chunkStart);
                processChunk(mappedByteBuffer, stats, chunkStart, chunkEnd != fileSize);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    private static long ensureChunkOffsetEndsWithCompleteLine(FileChannel fileChannel, long end, long fileSize) throws IOException {

        if (end >= fileSize) {
            return end;
        }

        fileChannel.position(end);
        var buffer = ByteBuffer.allocate(1);
        while (buffer.get() != '\n') {
            end++;
            if (end >= fileSize) {
                break;
            }
            buffer.clear();
            fileChannel.read(buffer);
            buffer.flip();
        }

        return end;
    }

    private static void processChunk(MappedByteBuffer buffer, Map<String, Statistics> stats, long startOffset, boolean hasNextChunk) {

        StringBuilder recordBuilder = new StringBuilder(64);
        boolean continueLastLine = startOffset != 0;

        while (buffer.hasRemaining()) {
            char c = (char) buffer.get();
            if (c == '\n') {
                if (continueLastLine) {
                    continueLastLine = false;
                }
                else {
                    parseLine(recordBuilder.toString(), stats);
                }
                recordBuilder.setLength(0); // Reset the builder
            }
            else if (!continueLastLine) {
                recordBuilder.append(c);
            }
        }

        // If this is not the last chunk, handle the partial line
        if (hasNextChunk && !recordBuilder.isEmpty()) {
            parseLine(recordBuilder.toString(), stats);
        }
    }

    private static void parseLine(String record, Map<String, Statistics> stats) {
        int pivot = record.indexOf(';');
        if (pivot == -1) {
            // Skip incomplete records
            return;
        }
        String key = record.substring(0, pivot);
        String value = record.substring(pivot + 1);
        try {
            double temp = parseTemperature(value);
            // double temp = Double.parseDouble(value);
            stats.merge(key, new Statistics(temp), Statistics::merge);
        }
        catch (Exception ex) {
            System.out.println("Could not parse record: " + record);
            throw ex;
        }
    }
}
