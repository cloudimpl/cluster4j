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

import java.io.IOException;
import java.math.RoundingMode;
import org.decimal4j.api.DecimalArithmetic;
import org.decimal4j.scale.ScaleMetrics;
import org.decimal4j.scale.Scales;
import org.decimal4j.truncate.TruncationPolicy;

/**
 *
 * @author nuwan
 */
public class ZeroGarbage {
	public static void main(String[] args) throws IOException {
            
            long v = 120;
            double v2 = v;
            
            System.out.println("v: "+v);
            System.out.println(Double.compare(v2, (double)v));
		ScaleMetrics scale3 = Scales.getScaleMetrics(3);
		DecimalArithmetic arith = scale3.getArithmetic(TruncationPolicy.DEFAULT);//.getArithmetic(RoundingMode.UP);
                        //getArithmetic(RoundingMode.UP)getDefaultArithmetic();
		
		long a = arith.fromLong(Long.MAX_VALUE);
		long b = arith.fromDouble(1.5);
		long c = arith.fromDouble(0.125);
		long d = arith.fromUnscaled(1, arith.getScale());

                long e = arith.fromUnscaled(400, 2);
                System.out.println("a:"+a + " e : "+e);
		System.out.println("ZERO GARBAGE: print values");
		System.out.print("a = ");arith.toString(a, System.out);
		System.out.println();
		System.out.print("b = ");arith.toString(b, System.out);
		System.out.println();
		System.out.print("c = ");arith.toString(c, System.out);
		System.out.println();
		System.out.print("d = ");arith.toString(d, System.out);
		System.out.println();
		
		System.out.println();
		System.out.println("ZERO GARBAGE: add values");
		long sumAB = arith.add(a, b);
		long sumCD = arith.add(c, d);
		System.out.print("a+b = ");arith.toString(sumAB, System.out);
		System.out.println();
		System.out.print("c+d = ");arith.toString(sumCD, System.out);
		System.out.println();
		
		System.out.println();
		System.out.println("ZERO GARBAGE: calculate average");
		long avgAB = arith.avg(a, b);
		long avgCD = arith.avg(c, d);
		long avgABCD = arith.divideByLong(arith.add(sumAB, sumCD), 4);
		System.out.print("(a+b)/2 = ");arith.toString(avgAB, System.out);
		System.out.println();
		System.out.print("(c+d)/2 = ");arith.toString(avgCD, System.out);
		System.out.println();
		System.out.print("(a+b+c+d)/4 = ");arith.toString(avgABCD, System.out);
		System.out.println();

		System.out.println();
		System.out.println("ZERO GARBAGE: round up/down");
		long avgBCdup = arith.deriveArithmetic(RoundingMode.UP).avg(b, c);
		long avgBCdown = arith.deriveArithmetic(RoundingMode.DOWN).avg(b, c);
		System.out.print("UP:   (b+c)/2 = ");arith.toString(avgBCdup, System.out);
		System.out.println();
		System.out.print("DOWN: (b+c)/2 = ");arith.toString(avgBCdown, System.out);
		System.out.println();

		System.out.println();
		System.out.println("ZERO GARBAGE: round to 2 decimal places");
		DecimalArithmetic arith2 = arith.deriveArithmetic(2);
		long rounded = arith.round(c, 2);
		long scaled = arith2.fromUnscaled(c, arith.getScale());
		System.out.print("round(c,2) = ");arith.toString(rounded, System.out);
		System.out.println();
		System.out.print("scale(c,2) = ");arith2.toString(scaled, System.out);
		System.out.println();
	}
}