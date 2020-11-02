/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.common;

/**
 *
 * @author nuwansa
 */
public enum RouterType {
  ROUND_ROBIN,
  DYNAMIC,
  DYNAMIC_AFFINTY,
  LOCAL,
  CONSISTENT_HASH,
  LEADER,
  LEADER_AFFINITY,
  SERVICE_ID,
  NODE_ID
}
