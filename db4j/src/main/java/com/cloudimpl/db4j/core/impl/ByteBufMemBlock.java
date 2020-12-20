/*
 * Copyright 2020 nuwansa.
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
package com.cloudimpl.db4j.core.impl;

import com.cloudimpl.db4j.core.MemBlock;

/** @author nuwansa */
public abstract class ByteBufMemBlock extends MemBlock {

  protected int offset;
  protected int maxItemCount;

  public ByteBufMemBlock(int offset, int maxItemCount) {
    this.offset = offset;
    this.maxItemCount = maxItemCount;
  }

  @Override
  public void setStartPos(long pos) {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  @Override
  public long getStartPos() {
    throw new UnsupportedOperationException(
        "Not supported yet."); // To change body of generated methods, choose Tools | Templates.
  }

  protected abstract int getKeySize();

  protected int getOffset() {
    return this.offset;
  }
}
