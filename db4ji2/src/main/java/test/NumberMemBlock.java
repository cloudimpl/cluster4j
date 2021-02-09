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
package test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.green.jelly.JsonNumber;
import org.green.jelly.MutableJsonNumber;

/**
 *
 * @author nuwan
 */
public class NumberMemBlock extends LongMemBlock {

    private long firstValue;
    private int maxExp = 0;
    private int queryMaxExp = 0;

    public NumberMemBlock(ByteBuffer byteBuf, int offset, int pageSize) {
        super(byteBuf, offset, pageSize);
    }

    public boolean put(LongBuffer temp, JsonNumber key, long value) {
        if (getSize() == 0) {
            firstValue = value;
        }
        int exp = Math.abs(key.exp());
        maxExp = Math.max(maxExp, exp);
        this.queryMaxExp = maxExp;
        long adjustVal = (int) (value - firstValue);
        long newVal = (adjustVal << 32) | (maxExp & 0xFFFFFFFFL);
        long newKey = key.mantissa() * lookupTable[(maxExp - exp)];
        return super.put(temp, newKey, newVal);
    }

    @Override
    public JsonNumber getKey(int index,MutableJsonNumber jsonNumber) {
        long mantissa = super.getKey(index);
        int exp = (int) super.getValue(index);
        jsonNumber.set(mantissa, -exp);
        return jsonNumber;
    }

    @Override
    public NumberEntry getEntry(int index,NumberEntry numberEntry) {
        getKey(index,numberEntry.key);
        numberEntry.value = getValue(index);
        return numberEntry;
    }

    @Override
    public long getKey(int index) {
        long mantissa = super.getKey(index);
        int exp = (int) super.getValue(index);
        int multiplier = this.queryMaxExp - exp;
        return mantissa * lookupTable[multiplier];
    }

    @Override
    public long getValue(int index) {
        return firstValue + ((super.getValue(index) >> 32) & 0xFFFFFFFFL);
    }

     @Override
    public int getMaxExp() {
        return this.maxExp;
    }
    
    @Override
    public Iterator all(Iterator ite) {
        return (Iterator) super.all(ite);
    }

    public Iterator findGE(Iterator ite, JsonNumber key) {
        int exp = Math.abs(key.exp());
        queryMaxExp = Math.max(queryMaxExp, exp);
        long newKey = key.mantissa() * lookupTable[(queryMaxExp - exp)];
        return super.findGE(ite, newKey);
    }

    public Iterator findGT(Iterator ite, JsonNumber key) {
        int exp = Math.abs(key.exp());
        queryMaxExp = Math.max(queryMaxExp, exp);
        long newKey = key.mantissa() * lookupTable[(queryMaxExp - exp)];
        return super.findGT(ite, newKey);
    }

    public Iterator findLE(Iterator ite, JsonNumber key) {
        int exp = Math.abs(key.exp());
        queryMaxExp = Math.max(queryMaxExp, exp);
        long newKey = key.mantissa() * lookupTable[(queryMaxExp - exp)];
        return super.findLE(ite, newKey);
    }

    public Iterator findLT(Iterator ite, JsonNumber key) {
        int exp = Math.abs(key.exp());
        queryMaxExp = Math.max(queryMaxExp, exp);
        long newKey = key.mantissa() * lookupTable[(queryMaxExp - exp)];
        return super.findLT(ite, newKey);
    }

    public Iterator findEQ(Iterator ite, JsonNumber key) {
        int exp = Math.abs(key.exp());
        queryMaxExp = Math.max(queryMaxExp, exp);
        long newKey = key.mantissa() * lookupTable[(queryMaxExp - exp)];
        return super.findEQ(ite, newKey);
    }

    public static JsonNumber[] toJsonNumber(List<BigDecimal> decimals) {
        return decimals.stream().map(d -> new MutableJsonNumber(d.unscaledValue().longValue(), -d.scale())).toArray(JsonNumber[]::new);
    }

    public static void main(String[] args) {
        ByteBuffer alloc = ByteBuffer.allocate(4096 * 10);
        NumberMemBlock longBlock = new NumberMemBlock(alloc, 0, 4096);
        LongBuffer temp = alloc.asLongBuffer();
        List<BigDecimal> decimals = IntStream.range(0, 255).mapToObj(i -> new BigDecimal(ThreadLocalRandom.current().nextDouble()).setScale(ThreadLocalRandom.current().nextInt(17), RoundingMode.CEILING)).collect(Collectors.toList());

        List<BigDecimal> list3 = new LinkedList<>(decimals);
        list3.sort(BigDecimal::compareTo);
        System.out.println(list3);
        //   list3.forEach(System.out::println);
        final JsonNumber[] list = toJsonNumber(decimals);//Arrays.asList(IntStream.range(1, 256).boxed().toArray());
        List<JsonNumber> list2 = new LinkedList<>(Arrays.asList(list));
        int i = 0;

        Iterator ite = new Iterator();
        // Collections.shuffle(list2);
        MutableJsonNumber json = new MutableJsonNumber();
        NumberEntry numberEntry = new NumberEntry();
        long start = System.currentTimeMillis();
        while (i < 1000000) {

            longBlock.updateSize(0);

            int q = 0;
            while (q < list2.size()) {

                //  json.set(list2.get(q), 0);
                //    System.out.println("json:"+json);
            //    System.out.println("insert: " + list2.get(q) + " decimal: " + decimals.get(q)+  " row : "+q);
                boolean ok = longBlock.put(temp, list2.get(q), q);
                if (!ok) {
                    System.out.println("Full");
                }
                q++;
            }

//            longBlock.all(ite);
//            q = 0;
//            int j = 0;
//            while (ite.hasNext()) {
//                int pos = ite.nextInt();
//                longBlock.getEntry(numberEntry, pos);
//                long unscale = list3.get(j).unscaledValue().longValue();
//                int exp = list3.get(j).scale();
//
//                int max = Math.max(Math.abs(numberEntry.key.exp()), exp);
//                long unscale2 = unscale * NumberMemBlock.lookupTable[max - exp];
//                long found = numberEntry.key.mantissa() * NumberMemBlock.lookupTable[max - Math.abs(numberEntry.key.exp())];
//                if (unscale2 != found) {
//                    System.out.println("entry:" + numberEntry + "  " + list3.get(j));
//                }
//
////                int j = (int) list.get(q);
////                if (j != numberEntry.key.mantissa()) {
////                    throw new RuntimeException("invalid :" + json + " j:" + j);
////                }
//                j++;
//                q++;
//            }
//            
//            json.set(5, -1);
//            Iterator ite2 = longBlock.findLE(ite, json);
//            ite2.forEachRemaining((int k)->System.out.println(longBlock.getEntry(numberEntry, k)));
//            //   System.out.println("q"+q);
//            if (true) {
//                break;
//            }
            i++;
        }
         long end = System.currentTimeMillis();
         System.out.println("op/s"+((double)(end -start)* 1000)/(i * list2.size()));
        System.out.println("longBuf: " + longBlock);
    }
}
