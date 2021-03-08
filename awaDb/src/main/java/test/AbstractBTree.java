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
import com.cloudimpl.mem.lib.OffHeapMemory;
import com.cloudimpl.mem.lib.MemHandler;
import java.nio.ByteOrder;
import java.util.function.Function;
import jdk.incubator.foreign.GroupLayout;
import jdk.incubator.foreign.MemoryLayout;
import jdk.incubator.foreign.SequenceLayout;
import org.agrona.BitUtil;

/**
 *
 * @author nuwan
 */
public abstract class AbstractBTree implements NumberQueryBlock {

    private final OffHeapMemory memorySegment;
    protected final int pageSize;
    protected final int maxItemCount;
    private final GroupLayout mainLayout;

    protected int currentItemIndex;

    protected final int idxNodeCapacity;
    protected final int keyNodeCapacity;
    protected final int valueNodeCapacity;

    protected final int idxNodeCount;
    protected final int keyNodeCount;
    protected final int valueNodeCount;

    protected final MemHandler idxItemHandler;
    protected final MemHandler keyItemHandler;
    protected final MemHandler valueItemHandler;
    protected final MemHandler minKeyHandler;
    protected final MemHandler maxKeyHandler;
    protected final MemHandler offsetHandler;
    protected final MemHandler maxExpHandler;

    protected final int keyIdxExponent;
    protected final int valueExponent;
    protected final int[] levelStartOffset;
    protected final int[] levelItemCount;
    protected long cacheStartOffset = -1;

    public AbstractBTree(int maxItemCount, int pageSize, int keySize, Class<?> keyType, int idxKeySize, Class<?> idxKeyType, int valueSize, Class<?> valueType, Function<Long, OffHeapMemory> memoryProvider) {
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
        SequenceLayout idxLayout = MemoryLayout.ofSequence(idxNodeCount, MemoryLayout.ofSequence(idxNodeCapacity, MemoryLayout.ofValueBits(idxKeySize * 8, ByteOrder.nativeOrder()).withName("idxItem")).withName("idxNodeLayout"));
        SequenceLayout keyLayout = MemoryLayout.ofSequence(keyNodeCount, MemoryLayout.ofSequence(keyNodeCapacity, MemoryLayout.ofValueBits(keySize * 8, ByteOrder.nativeOrder()).withName("keyItem")).withName("keyNodeLayout"));
        SequenceLayout valueLayout = MemoryLayout.ofSequence(valueNodeCount, MemoryLayout.ofSequence(valueNodeCapacity, MemoryLayout.ofValueBits(valueSize * 8, ByteOrder.nativeOrder()).withName("valueItem")).withName("valueNodeLayout"));

        //MemoryLayout.ofStruct(MemoryLayout.ofSequence(maxItemPerNode, MemoryLayout.ofValueBits(keySize * 8, ByteOrder.nativeOrder())).withName("keySequence"),
        // MemoryLayout.ofSequence(maxItemPerNode, MemoryLayout.ofValueBits(Integer.SIZE, ByteOrder.nativeOrder())).withName("valueSequence")));
        this.mainLayout = MemoryLayout.ofStruct(paddingLayout.withName("headerLayout"), idxLayout.withName("idxLayout"), keyLayout.withName("keyLayout"), valueLayout.withName("valueLayout"));
        this.memorySegment = memoryProvider.apply(this.mainLayout.byteSize());
        this.idxItemHandler = this.memorySegment.memHandler(idxKeyType,mainLayout,"idxLayout", MemoryLayout.PathElement.groupElement("idxLayout"), MemoryLayout.PathElement.sequenceElement(), MemoryLayout.PathElement.sequenceElement());
        this.keyItemHandler = this.memorySegment.memHandler(keyType,mainLayout,"keyLayout", MemoryLayout.PathElement.groupElement("keyLayout"), MemoryLayout.PathElement.sequenceElement(), MemoryLayout.PathElement.sequenceElement());
        this.valueItemHandler = this.memorySegment.memHandler(valueType,mainLayout,"valueLayout", MemoryLayout.PathElement.groupElement("valueLayout"), MemoryLayout.PathElement.sequenceElement(), MemoryLayout.PathElement.sequenceElement());
        this.maxKeyHandler = this.memorySegment.memHandler(idxKeyType == double.class ? double.class : long.class,headerLayout,"max", MemoryLayout.PathElement.groupElement("max"));
        this.minKeyHandler = this.memorySegment.memHandler(idxKeyType == double.class ? double.class : long.class,headerLayout,"min", MemoryLayout.PathElement.groupElement("min"));
        this.offsetHandler = this.memorySegment.memHandler(long.class,headerLayout,"startOffset", MemoryLayout.PathElement.groupElement("startOffset"));
        this.maxExpHandler = this.memorySegment.memHandler(long.class,headerLayout,"startOffset", MemoryLayout.PathElement.groupElement("maxExp"));
        
        int levelCount = getIndexLevelCount(0, keyNodeCount);
        this.levelStartOffset = new int[levelCount];
        this.levelItemCount = new int[levelCount];
        this.keyIdxExponent = Math.getExponent(this.idxNodeCapacity);
        this.valueExponent = Math.getExponent(this.valueNodeCapacity);
//        System.out.println("levels:" + levelCount + " "+idxLayout.attribute(MemoryLayout.LAYOUT_NAME));
//        System.out.println("padding offset:"+mainLayout.memberLayouts().stream().filter(l->l.name().get().equals("idxLayout")).findAny().get().byteOffset());
//        
//        System.out.println("keyLayout offset:"+this.mainLayout.byteOffset(MemoryLayout.PathElement.groupElement("keyLayout")));
//        
//        System.out.println("valueLayout offset:"+this.mainLayout.byteOffset(MemoryLayout.PathElement.groupElement("valueLayout")));
//        
//        System.out.println("valueLayout offset:"+headerLayout.byteOffset(MemoryLayout.PathElement.groupElement("startOffset")));
        
        //this.mainLayout.memberLayouts().forEach(s->System.out.println(s.name() + ":" +s.byteSize()));
//        print(mainLayout,MemoryLayout.PathElement.groupElement("idxLayout"), MemoryLayout.PathElement.sequenceElement(), MemoryLayout.PathElement.sequenceElement());
     //   System.out.println("btree size : "+this.memorySegment.size() + " type : "+keyType);
    }

    private void print(MemoryLayout layout,MemoryLayout.PathElement...  elements)
    {
        System.out.println("name:" + layout.name() + " : "+layout.byteSize() + " offset: " + (elements.length > 0 ?layout.byteOffset(elements[0]):-1));
        System.out.println("[");
        if(layout instanceof GroupLayout)
        {
            GroupLayout groupLayout = (GroupLayout)layout;
            groupLayout.memberLayouts().forEach(this::print);
        }
        else if(layout instanceof SequenceLayout)
        {
            SequenceLayout seqLayout = (SequenceLayout)layout;
       //     System.out.println("name:"+seqLayout.elementLayout().name()+" : "+seqLayout.elementLayout().byteSize());
             System.out.println("el count:"+seqLayout.elementCount());
            print(seqLayout.elementLayout());
        }
//        else if(layout instanceof ValueLayout)
//        {
//            ValueLayout valueLayout = (ValueLayout)layout;
//            System.out.println("name:"+valueLayout.name()+" : "+valueLayout.byteSize());
//        }
         System.out.println("]");
    }
    protected final void setStartingOffset(long value) {
        this.offsetHandler.set(value);
        this.cacheStartOffset = value;
    }

    protected void setValue(long value) {
        long valueNodeIdx = currentItemIndex >> this.valueExponent;
        long valueitemIdx = currentItemIndex & (this.valueNodeCapacity - 1);
        this.valueItemHandler.set(valueNodeIdx, valueitemIdx, (int) (value - getStartingOffset()));
    }

    protected final long getStartingOffset() {
        if (this.cacheStartOffset == -1) {
            this.cacheStartOffset = this.offsetHandler.getLong();
        }
        return this.cacheStartOffset;
    }

    @Override
    public long memSize() {
        return this.memorySegment.size();
    }

    @Override
    public final int getSize() {
        return this.currentItemIndex;
    }

    @Override
    public Iterator all(Iterator ite) {
        return ite.init(this, 0, getSize());
    }

    @Override
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
        return (long) getStartingOffset() + (this.valueItemHandler.getInt(nodeIdx, itemIdx));
    }

    protected long getRowValue(int index) {
        int nodeIdx = index >> this.valueExponent;
        int itemIdx = index & this.valueNodeCapacity - 1;
        return this.valueItemHandler.getLong(nodeIdx, itemIdx);
    }

    private int fillLevel(int dstItemOffset, MemHandler itemHandler, int srcNodeIdx, final int itemCount) {
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

    protected abstract void fillIdxNode(long dstNodeIdx, long dstItemIdx, MemHandler itemHandler, long srcNodeIdx, long srcItemIdx);

    private void fillLevels(int level, int dstItemOffset, MemHandler itemHandler, int srcNodeIdx, int nodesCount) {
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
       // System.out.println("level:" + count);
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
