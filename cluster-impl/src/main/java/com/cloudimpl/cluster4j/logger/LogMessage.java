package com.cloudimpl.cluster4j.logger;

import com.cloudimpl.cluster4j.coreImpl.TimeUtils;
import java.io.Serializable;

/**
 * This is representing the message message.
 */
public final class LogMessage implements Serializable {

  private final String group;
  private final String section;
  private final String host;
  private final String nodeId;
  private final String level;
  private final long time;
  private final String message;

  public LogMessage(String group, String section, String host, String nodeId, String level, long time, String message) {
    this.group = group;
    this.section = section;
    this.host = host;
    this.nodeId = nodeId;
    this.level = level;
    this.time = time;
    this.message = message;
  }

  /**
   * Gets the message message owner group.
   *
   * @return the message message group
   */
  public String getGroup() {
    return group;
  }

  /**
   * Gets the message message owner section.
   *
   * @return the message message section
   */
  public String getSection() {
    return section;
  }

  /**
   * Gets the message message created host.
   *
   * @return the message message host
   */
  public String getHost() {
    return host;
  }

  /**
   * Gets the message message level.
   *
   * @return the message message level
   */
  public String getLevel() {
    return level;
  }

  /**
   * Gets the message message created timestamp.
   *
   * @return the message message time
   */
  public long getTime() {
    return time;
  }

  /**
   * Gets the message of the log.
   *
   * @return the message
   */
  public String getMessage() {
    return message;
  }

  public String getNodeId() {
    return nodeId;
  }

  @Override
  public String toString() {
    return TimeUtils.fromEpoch(time).toString("yyyy/MM/dd HH:mm:ss.SSS") + "|"
        + host + "|" + nodeId + "|" + String.format("%1$-5s", level) + "|" + group + "-" + section + "|" + message;
  }
}
