package com.pingcap.ch.columns;

import shade.com.google.common.base.Preconditions;
import com.pingcap.ch.columns.UTF8ChunkBuilder.SealedChunk;
import java.util.ArrayList;
import java.util.List;
import org.apache.spark.unsafe.types.UTF8String;

public class PagedChunkBuilder {

    private UTF8ChunkBuilder pageBuf;
    private List<SealedChunk> chunkPages;

    public PagedChunkBuilder(int pageCapacity) {
        Preconditions.checkArgument(pageCapacity > 0, "Page capacity must be positive");
        this.chunkPages = new ArrayList<>();

        pageBuf = new UTF8ChunkBuilder(pageCapacity);
    }

    public void insertUTF8String(UTF8String v) {
        // The passed in string could be a pointer.
        if (pageBuf.isFull()) {
            chunkPages.add(pageBuf.seal());
        }
        pageBuf.append(v);
    }

    public SealedChunk seal() {
        if (!pageBuf.isEmpty()) {
            chunkPages.add(pageBuf.seal());
        }
        return UTF8ChunkBuilder.mergeChunk(chunkPages);
    }
}
