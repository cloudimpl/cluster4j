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

import com.cloudimpl.db4ji2.idx.str.StringPage;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.function.Function;

/**
 *
 * @author nuwan
 */
public class ReadOnlyStringBlock {
    private final Function<Integer,ByteBuffer> bufferProvider;
    private final ArrayList<StringPage> pages;
    private StringPage current;
    private int currentIndex;
    private int pageSize;
    public ReadOnlyStringBlock(int pageSize,Function<Integer, ByteBuffer> bufferProvider) {
        this.pageSize = pageSize;
        this.bufferProvider = bufferProvider;
        this.pages = new ArrayList<>();
        this.current = null;
        this.currentIndex = -1;
    }
}
