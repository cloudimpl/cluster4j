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
package com.cloudimpl.db4j.core.btree2;

import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author nuwansa */
public class LeafNodeImplTest {

  LeafNodeImpl leafNode1;
  LeafNodeImpl leafNode2;

  public LeafNodeImplTest() {}

  @BeforeClass
  public static void setUpClass() {}

  @AfterClass
  public static void tearDownClass() {}

  @Before
  public void setUp() {
    ByteBuffer buf = ByteBuffer.allocate(4096 * 2);
    leafNode1 =
        new LeafNodeImpl(
            buf, buf.slice().asLongBuffer().limit(ByteBufNode.capacity(4096)), 253, 4096, 0);
    leafNode1.setKey(0, 1);
    leafNode1.setValue(0, 10);
    leafNode1.setSize(1);
    leafNode2 =
        new LeafNodeImpl(
            buf,
            buf.position(4096).slice().asLongBuffer().limit(ByteBufNode.capacity(4096)),
            253,
            4096,
            4096);
    leafNode2.setKey(0, 10);
    leafNode2.setValue(0, 20);
    leafNode2.setSize(1);
  }

  @After
  public void tearDown() {}

  /** Test of getValue method, of class LeafNodeImpl. */
  @Test
  public void testGetValue() {
    assertEquals(10, leafNode1.getValue(0));
  }

  /** Test of getKey method, of class LeafNodeImpl. */
  @Test
  public void testGetKey() {
    assertEquals(1, leafNode1.getKey(0));
  }

  /** Test of next method, of class LeafNodeImpl. */
  @Test
  public void testNext() {
    LeafNode node = leafNode1.next();
    assertNull(node);
    leafNode1.setNext();
    node = leafNode1.next();
    assertNotNull(node);
    assertEquals(10, node.getKey(0));
    assertEquals(20, node.getValue(0));
    assertEquals(1, node.size());
  }

  /** Test of previous method, of class LeafNodeImpl. */
  @Test
  public void testPrevious() {
    LeafNode node = leafNode2.previous();
    assertNull(node);
    leafNode2.setPrevious();
    node = leafNode2.previous();
    assertNotNull(node);
    assertEquals(1, node.getKey(0));
    assertEquals(10, node.getValue(0));
    assertEquals(1, node.size());
  }

  /** Test of size method, of class LeafNodeImpl. */
  @Test
  public void testSize() {
    assertEquals(1, leafNode1.size());
    leafNode1.setSize(3);
    assertEquals(3, leafNode1.size());
  }

  /** Test of setValue method, of class LeafNodeImpl. */
  @Test
  public void testSetValue() {
    leafNode1.setKey(1, 3);
    leafNode1.setValue(1, 14);
    assertEquals(14, leafNode1.getValue(1));
  }

  /** Test of setKey method, of class LeafNodeImpl. */
  @Test
  public void testSetKey() {
    leafNode1.setKey(1, 3);
    leafNode1.setValue(1, 14);
    assertEquals(3, leafNode1.getKey(1));
  }
}
