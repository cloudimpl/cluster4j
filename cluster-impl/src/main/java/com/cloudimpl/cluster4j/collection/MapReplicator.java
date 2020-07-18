/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.collection;

import com.cloudimpl.cluster4j.common.Pair;
import com.cloudimpl.cluster4j.node.CloudNode;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 *
 * @author nuwansa
 */
public class MapReplicator {
  private CloudNode node;
  private Queue<Pair<Object, Object>> queue = new LinkedBlockingDeque<>();
}
