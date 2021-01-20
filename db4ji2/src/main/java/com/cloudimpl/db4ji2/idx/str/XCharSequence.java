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
package com.cloudimpl.db4ji2.idx.str;

/**
 *
 * @author nuwan
 */
public interface XCharSequence {
    char nextChar(long pos);
    
    static int compare(long l,XCharSequence left,long r,XCharSequence right)
    {
        char lc = left.nextChar(l++);
        char rc = right.nextChar(r++);
        while (lc != StringBlock.NULL && rc != StringBlock.NULL) {
            if (lc != rc) {
                return lc - rc;
            }
            lc = left.nextChar(l++);
            rc = right.nextChar(r++);
        }

        if (lc != StringBlock.NULL && rc == StringBlock.NULL) {
            System.out.println("l:" + left + "> r: " + right);
            return lc;
        } else if (lc == StringBlock.NULL && rc != StringBlock.NULL) {
            System.out.println("l:" + left + "< r: " + right);
            return -rc;
        } else if (lc == StringBlock.NULL && rc == StringBlock.NULL) {
            //     System.out.println("l:"+left+ "= r: "+right);
            return 0;
        }
        throw new RuntimeException("unknown size");
    }
}
