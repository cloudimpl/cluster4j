package test;

/*
 * Copyright 2021 nuwan.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.cloudimpl.db4ji2.core.old.Validation;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.function.Function;
import jdk.incubator.foreign.GroupLayout;
import jdk.incubator.foreign.MemoryAddress;
import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.MemorySegment;
import jdk.incubator.foreign.SequenceLayout;
import org.agrona.BitUtil;

/**
 *
 * @author nuwan
 */
public abstract class AbstractBTree implements NumberQueryBlock {

    protected final MemorySegment memorySegment;
    protected final int pageSize;
    protected final int maxItemCount;
    protected final MemoryAddress address;
    protected final GroupLayout mainLayout;

    protected int currentItemIndex;

    protected final int idxNodeCapacity;
    protected final int keyNodeCapacity;
    protected final int valueNodeCapacity;

    protected final int idxNodeCount;
    protected final int keyNodeCount;
    protected final int valueNodeCount;

    protected final VarHandle idxItemHandler;
    protected final VarHandle keyItemHandler;
    private final VarHandle valueItemHandler;
    protected final VarHandle minKeyHandler;
    protected final VarHandle maxKeyHandler;
    protected final VarHandle offsetHandler;
    protected final VarHandle maxExpHandler;

    protected final int keyIdxExponent;
    protected final int valueExponent;
    protected final int[] levelStartOffset;
    protected final int[] levelItemCount;
    protected long cacheStartOffset = -1;

    public AbstractBTree(int maxItemCount, int pageSize, int keySize, Class<?> keyType,int valueSize,Class<?> valueType, Function<MemoryLayout, MemorySegment> memoryProvider) {
        this.maxItemCount = maxItemCount;
        this.pageSize = pageSize;
        this.idxNodeCapacity = pageSize / keySize;
        this.keyNodeCapacity = pageSize / keySize;
        this.valueNodeCapacity = pageSize / Integer.BYTES;

        Validation.checkCondition((pageSize & 7) == 0, () -> "pageSize should be multiple of 8");
        Validation.checkCondition(BitUtil.isPowerOfTwo(keyNodeCapacity), () -> "keyNodeCapacity should be two to the power:" + keyNodeCapacity);
        Validation.checkCondition(BitUtil.isPowerOfTwo(idxNodeCapacity), () -> "idxNodeCapacity should be two to the power:" + idxNodeCapacity);
        Validation.checkCondition(BitUtil.isPowerOfTwo(valueNodeCapacity), () -> "valueNodeCapacity capacity should be two to the power" + valueNodeCapacity);

        this.keyNodeCount = getKeyNodeCount();
        this.idxNodeCount = getTotalIndexNodeCount(keyNodeCount);
        this.valueNodeCount = getValueNodeCount();

        GroupLayout headerLayout = MemoryLayout.ofStruct(
                MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("version"),
                MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("pageSize"),
                MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("maxItemCount"),
                MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("startOffset"),
                MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("min"),
                MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("max"),
                MemoryLayout.ofValueBits(Long.SIZE, ByteOrder.nativeOrder()).withName("maxExp")
        );
        GroupLayout paddingLayout = MemoryLayout.ofUnion(MemoryLayout.ofPaddingBits(256 * 8), headerLayout);
        SequenceLayout idxLayout = MemoryLayout.ofSequence(idxNodeCount, MemoryLayout.ofSequence(idxNodeCapacity, MemoryLayout.ofValueBits(keySize * 8, ByteOrder.nativeOrder())));
        SequenceLayout keyLayout = MemoryLayout.ofSequence(keyNodeCount, MemoryLayout.ofSequence(keyNodeCapacity, MemoryLayout.ofValueBits(keySize * 8, ByteOrder.nativeOrder())));
        SequenceLayout valueLayout = MemoryLayout.ofSequence(valueNodeCount, MemoryLayout.ofSequence(valueNodeCapacity, MemoryLayout.ofValueBits(valueSize * 8, ByteOrder.nativeOrder())));

        //MemoryLayout.ofStruct(MemoryLayout.ofSequence(maxItemPerNode, MemoryLayout.ofValueBits(keySize * 8, ByteOrder.nativeOrder())).withName("keySequence"),
        // MemoryLayout.ofSequence(maxItemPerNode, MemoryLayout.ofValueBits(Integer.SIZE, ByteOrder.nativeOrder())).withName("valueSequence")));
        this.mainLayout = MemoryLayout.ofStruct(paddingLayout.withName("headerLayout"), idxLayout.withName("idxLayout"), keyLayout.withName("keyLayout"), valueLayout.withName("valueLayout"));
        this.memorySegment = memoryProvider.apply(this.mainLayout);
        this.idxItemHandler = this.mainLayout.varHandle(keyType, MemoryLayout.PathElement.groupElement("idxLayout"), MemoryLayout.PathElement.sequenceElement(), MemoryLayout.PathElement.sequenceElement());
        this.keyItemHandler = this.mainLayout.varHandle(keyType, MemoryLayout.PathElement.groupElement("keyLayout"), MemoryLayout.PathElement.sequenceElement(), MemoryLayout.PathElement.sequenceElement());
        this.valueItemHandler = this.mainLayout.varHandle(valueType, MemoryLayout.PathElement.groupElement("valueLayout"), MemoryLayout.PathElement.sequenceElement(), MemoryLayout.PathElement.sequenceElement());
        this.maxKeyHandler = headerLayout.varHandle(long.class, MemoryLayout.PathElement.groupElement("max"));
        this.minKeyHandler = headerLayout.varHandle(long.class, MemoryLayout.PathElement.groupElement("min"));
        this.offsetHandler = headerLayout.varHandle(long.class, MemoryLayout.PathElement.groupElement("startOffset"));
        this.maxExpHandler = headerLayout.varHandle(long.class, MemoryLayout.PathElement.groupElement("maxExp"));
        this.address = this.memorySegment.baseAddress();
        int levelCount = getIndexLevelCount(0, keyNodeCount);
        this.levelStartOffset = new int[levelCount];
        this.levelItemCount = new int[levelCount];
        this.keyIdxExponent = Math.getExponent(this.idxNodeCapacity);
        this.valueExponent = Math.getExponent(this.valueNodeCapacity);
        System.out.println("levels:" + levelCount);
    }

    protected final void setStartingOffset(long value) {
        this.offsetHandler.set(this.address, value);
        this.cacheStartOffset = value;
    }

    protected final void setValue(long value) {
        long valueNodeIdx = currentItemIndex >> this.valueExponent;
        long valueitemIdx = currentItemIndex & (this.valueNodeCapacity - 1);
        this.valueItemHandler.set(this.address, valueNodeIdx, valueitemIdx, (int) (value - getStartingOffset()));
    }
    
    protected final long getStartingOffset() {
        if (this.cacheStartOffset == -1) {
            this.cacheStartOffset = (long) this.offsetHandler.get(this.address);
        }
        return this.cacheStartOffset;
    }

    public long memSize() {
        return this.memorySegment.byteSize();
    }

    @Override
    public final int getSize() {
        return this.currentItemIndex;
    }

    @Override
    public Iterator all(Iterator ite) {
        return ite.init(this, 0, getSize());
    }

    public void close() {
        this.memorySegment.close();
    }

    public void reset() {
        this.currentItemIndex = 0;
    }

    @Override
    public long getValue(int index) {
        int nodeIdx = index >> this.valueExponent;
        int itemIdx = index & this.valueNodeCapacity - 1;
        return (long) getStartingOffset() + ((int) this.valueItemHandler.get(this.address, nodeIdx, itemIdx));
    }

    private int fillLevel(int dstItemOffset, VarHandle itemHandler, int srcNodeIdx, final int itemCount) {
        int i = 1;
        while (i < itemCount) {
            int nodeIdx = dstItemOffset >> this.keyIdxExponent;
            int itemIdx = dstItemOffset & this.idxNodeCapacity - 1;
            fillIdxNode(nodeIdx, itemIdx, itemHandler, ++srcNodeIdx, 0);
            dstItemOffset++;
            i++;
        }
        return getLevelIndexCount(itemCount);
    }

    protected abstract void fillIdxNode(long dstNodeIdx, long dstItemIdx, VarHandle itemHandler, long srcNodeIdx, long srcItemIdx);

    private void fillLevels(int level, int dstItemOffset, VarHandle itemHandler, int srcNodeIdx, int nodesCount) {
        this.levelStartOffset[level - 1] = dstItemOffset;
        int levelCount = fillLevel(dstItemOffset, itemHandler, srcNodeIdx, nodesCount);
        this.levelItemCount[level - 1] = nodesCount - 1;
        if (dstItemOffset == 0) {
            return;
        }
        fillLevels(level - 1, dstItemOffset - (getLevelIndexCount(levelCount) * this.idxNodeCapacity), idxItemHandler, dstItemOffset >> this.keyIdxExponent, levelCount);
    }

    public void complete() {
        fillLevels(this.levelStartOffset.length, (idxNodeCount - getLevelIndexCount(keyNodeCount)) * this.idxNodeCapacity, keyItemHandler, 0, keyNodeCount);
    }

    protected final int getKeyNodeCount() {
        return (int) Math.ceil(((double) maxItemCount) / keyNodeCapacity);
    }

    protected final int getValueNodeCount() {
        return (int) Math.ceil(((double) maxItemCount) / valueNodeCapacity);
    }

    protected final int getLevelIndexCount(int maxItemCount) {
        return (int) Math.ceil(((double) maxItemCount - 1) / (idxNodeCapacity));
    }

    protected final int getTotalIndexNodeCount(int maxItemCount) {
        int count = getLevelIndexCount(maxItemCount);
        System.out.println("level:" + count);
        if (count <= 1) {
            return 1;
        }

        return count + getTotalIndexNodeCount(count);
    }

    protected final int getIndexLevelCount(int i, int maxItemCount) {
        int idxCount = getLevelIndexCount(maxItemCount);
        if (idxCount == 1) {
            return i + 1;
        }
        return getIndexLevelCount(i + 1, idxCount);
    }

}
