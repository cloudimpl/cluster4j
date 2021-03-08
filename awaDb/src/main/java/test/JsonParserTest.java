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

import org.green.jelly.JsonNumber;
import org.green.jelly.JsonParser;
import org.green.jelly.JsonParserListener;

/**
 *
 * @author nuwan
 */
public class JsonParserTest {

    public static void main(String[] args) {
        System.out.println(Double.MAX_VALUE + " - "+ Long.MAX_VALUE);
         JsonParser parser = new JsonParser();
        parser.setListener(new JsonParserListener() {
            @Override
            public void onJsonStarted() {
                System.out.println("json started");
                //      throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public void onError(String error, int position) {
                //       throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                System.out.println("error:"+error+" pos:"+position);
            }

            @Override
            public void onJsonEnded() {
                System.out.println("json end");
                //         throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
            }

            @Override
            public boolean onObjectStarted() {
                //        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                System.out.println("object started");
                return true;
            }

            @Override
            public boolean onObjectMember(CharSequence name) {
                //       throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
          //      System.out.println("member:" + name);
                return true;
            }

            @Override
            public boolean onObjectEnded() {
                //        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                System.out.println("object end");
                return true;
            }

            @Override
            public boolean onArrayStarted() {
                //         throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                return true;
            }

            @Override
            public boolean onArrayEnded() {
                //        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                return true;
            }

            @Override
            public boolean onStringValue(CharSequence data) {
                //       throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                System.out.println("value :"+data);
                return true;
            }

            @Override
            public boolean onNumberValue(JsonNumber number) {
                //        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                System.out.println("val: "+number.mantissa()+"exp: "+number.exp());
                return true;
            }

            @Override
            public boolean onTrueValue() {
                //       throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                System.out.println("true val:");
                return true;
            }

            @Override
            public boolean onFalseValue() {
                //          throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                System.out.println("false val:");
                return true;
            }

            @Override
            public boolean onNullValue() {
                //         throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                System.out.println("null val:");
                return true;
            }
        });
        
//         List<BigDecimal> decimals = IntStream.range(0, 256).mapToObj(i->new BigDecimal(ThreadLocalRandom.current().nextDouble()).setScale(ThreadLocalRandom.current().nextInt(17),RoundingMode.CEILING)).collect(Collectors.toList());
//         List<BigDecimal> sorts = new LinkedList<>(decimals);
//         sorts.sort(BigDecimal::compareTo);
//         JsonNumber[] numbers = NumberMemBlock.toJsonNumber(decimals);
//         
//         JsonNumber[] newNumbers = new JsonNumber[numbers.length];
//         int maxExp =0;
//         int i = 0;
//         for(JsonNumber num : numbers)
//         {
//             long mantissa = num.mantissa();
//             int exp = Math.abs(num.exp());
//             maxExp = Math.max(exp,maxExp);
//             mantissa = mantissa * NumberMemBlock.lookupTable[maxExp - exp];
//             newNumbers[i++] = new MutableJsonNumber(mantissa, maxExp);
//         }
//         int finalMax = maxExp;
//        Arrays.sort(numbers,(JsonNumber left, JsonNumber right)->{
//            
//            int leftExp = Math.abs(left.exp());
//            int rightExp = Math.abs(right.exp());
//            int max = Math.max(leftExp, rightExp);
//            long leftVal = left.mantissa() * NumberMemBlock.lookupTable[max - leftExp];
//            
//            long rightVal = right.mantissa() * NumberMemBlock.lookupTable[max - rightExp];
//            return Long.compare(leftVal,rightVal);
//        });
//        
//        IntStream.range(0, newNumbers.length).forEach(k->System.out.println(sorts.get(k)+" : "+numbers[k]));
       //  sorts.forEach(System.out::println);
         
         
         
        CharSequence seq = "{\"name\" :";
        CharSequence seq2 = "0.10}";
        while(true)
        {
            parser.parse(seq);
            parser.parse(seq2);
            parser.reset();
        }
        
    }
}
