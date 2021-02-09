///*
// * Copyright 2021 nuwan.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package test;
//
//import jdk.incubator.vector.FloatVector;
//import jdk.incubator.vector.LongVector;
//import jdk.incubator.vector.VectorMask;
//import jdk.incubator.vector.VectorSpecies;
//
///**
// *
// * @author nuwan
// */
//public class VectorTest {
//
//    static final VectorSpecies<Long> SPECIES = LongVector.SPECIES_256;
//
//    public static void scalarComputation(long[] a, long[] b, long[] c) {
//        for (int i = 0; i < a.length; i++) {
//            c[i] = (a[i] * a[i] + b[i] * b[i]);
//        }
//    }
//
//    public static void vectorComputation(long[] a, long[] b, long[] c) {
//
//        for (int i = 0; i < a.length; i += SPECIES.length()) {
//            VectorMask<Long> m = SPECIES.indexInRange(i, a.length);
//            // FloatVector va, vb, vc;
//            LongVector va  = LongVector.fromArray(SPECIES, a, i, m);
//            LongVector vb = LongVector.fromArray(SPECIES, b, i, m);
//            LongVector vc = va.mul(va).
//                    add(vb.mul(vb));
//            vc.intoArray(c, i, m);
//        }
//    }
//
//    public static void main(String[] args) {
//        long[] a = new long[30000000];
//        long[] b = new long[30000000];
//        long[] c = new long[30000000];
//
//        for (int i = 0; i < a.length; i++) {
//            a[i] = i;
//            b[i] = i * 10;
//        }
//        int i = 0;
//        while (i < 1000) {
//            long s = System.currentTimeMillis();
//            scalarComputation(a, b, c);
//            long e = System.currentTimeMillis();
//            System.out.println("time scalar:" + (e - s));
//            s = System.currentTimeMillis();
//            vectorComputation(a, b, c);
//            e = System.currentTimeMillis();
//            System.out.println("time vector:" + (e - s));
//            i++;
//        }
//
//    }
//}
