/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.le;

import com.cloudimpl.cluster4j.core.FluxMap;
import com.cloudimpl.cluster4j.common.Pair;
import com.cloudimpl.cluster4j.core.logger.ILogger;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author nuwansa
 */
public class LeaderElection {

  public enum MemberStatus {
    LEADER,
    FOLLOWER,
    CANDIDATE
  }

  private final Map<String, LeaderInfo> dataStore;
  private final String memberId;
  private final String leaderGroup;
  private final Listener listener;
  private int count = 0;
  public MemberStatus status;
  private LeaderInfo leaderInfo;

  private final ScheduledExecutorService schedular;

  private final Set<String> members;
  private final Queue<Pair<Boolean, String>> pendingMembers;
  private final FluxMap<String, LeaderInfo> leaderMap;
  private long leaderExpirePeriod = 10000;
  private ILogger logger;

  protected LeaderElection(String leaderGroup, String memberId, Map<String, LeaderInfo> dataStore,
      long leaderExpirePeriod,
      Listener listener, FluxMap<String, LeaderInfo> leaderMap, ILogger logger) {
    this.logger = logger.createSubLogger("LeaderElection", leaderGroup);
    this.dataStore = dataStore;
    this.leaderMap = leaderMap;
    this.memberId = memberId;
    this.leaderInfo = null;
    this.listener = listener;
    this.leaderGroup = leaderGroup;
    this.status = MemberStatus.FOLLOWER;
    this.schedular = Executors.newSingleThreadScheduledExecutor();
    this.members = new ConcurrentSkipListSet<>();
    this.members.add(memberId);
    this.pendingMembers = new ConcurrentLinkedQueue<>();
    this.leaderExpirePeriod = leaderExpirePeriod;
  }

  public void run() {
    schedular.scheduleAtFixedRate(this::onTick, 1, 1, TimeUnit.SECONDS);
  }

  protected void onLeaderChange(LeaderInfo info) {
    logger.info("leader change called {0}", info);
    if (members.contains(info.getLeaderId())) {
      logger.info("leader change callback called {0}", info);
      listener.leaderChange(this, info.getLeaderId());

      if (leaderMap != null)
        leaderMap.put(leaderGroup, info);
    }
  }

  public void addMember(String memberId) {
    logger.info("add member {0}", memberId);
    pendingMembers.add(new Pair<>(true, memberId));
  }

  public void removeMember(String memberId) {
    logger.info("remove member {0}", memberId);
    pendingMembers.add(new Pair<>(false, memberId));
  }

  private boolean isMember(String memberId) {
    return members.contains(memberId);
  }

  public long getLeaderExpirePeriod() {
    return leaderExpirePeriod;
  }

  private void processQueue() {
    Pair<Boolean, String> pair;
    while ((pair = pendingMembers.poll()) != null) {
      boolean ok;
      if (pair.getKey()) {
        ok = members.add(pair.getValue());
      } else {
        ok = members.remove(pair.getValue());
      }
      if (leaderInfo != null) {
        if (pair.getKey() && ok) {
          if (leaderInfo.getLeaderId().equals(pair.getValue())) {
            onLeaderChange(leaderInfo);
          }
        } else if (ok) {
          if (leaderInfo.getLeaderId().equals(pair.getValue())) {
            status = MemberStatus.CANDIDATE;
          }
        }
      }
    }
  }

  protected void onTick() {
    logger.debug("tick");
    processQueue();
    try {
      LeaderInfo newInfo = null;
      switch (status) {
        case FOLLOWER:
          logger.debug("follower leaderInfo {0}", leaderInfo);
          if (leaderInfo == null) {
            newInfo = loadLeader(); // load the leader here
            if (newInfo != null) {
              onLeaderChange(newInfo);
            } else {
              status = MemberStatus.CANDIDATE; // no leader found , become a candidate
            }
          } else if (!isMember(leaderInfo.leaderId)) {
            status = MemberStatus.CANDIDATE; // leader not in the member list
          }
          break;
        case CANDIDATE:
          logger.info("candidate leaderInfo {0}", leaderInfo);
          newInfo = becomeLeader(leaderInfo != null);
          if (leaderInfo == null) {
            onLeaderChange(newInfo);
          } else {
            if (!leaderInfo.getLeaderId().equals(newInfo.getLeaderId())) {
              onLeaderChange(newInfo);
            }
          }
          break;
        case LEADER:
          logger.debug("leader leaderInfo {0}", leaderInfo);
          count++;
          if (count >= 2) {
            newInfo = updateLeader();
            if (newInfo != null && !isLeaderValid(newInfo)) {
              logger.info("leader hb failed no valid leader found.become candidate {0}", leaderInfo);
              status = MemberStatus.CANDIDATE; // stale leader found. become a candidate.
              leaderInfo = newInfo;
              newInfo = null;
            } else if (newInfo != null) {
              logger.info("leader hb failed,another valid leader found {0}", leaderInfo);
              onLeaderChange(newInfo);
            }
            count = 0;
          }
          break;
        default:
          break;
      }

      if (newInfo != null) {
        logger.info("new leader info returned {0}", newInfo);
        if (newInfo.getLeaderId().equals(memberId)) {
          logger.info("me become leader", newInfo);
          status = MemberStatus.LEADER;
          count = 0;
        } else {
          logger.info("me become follower", newInfo);
          status = MemberStatus.FOLLOWER;
        }
        leaderInfo = newInfo;
      }
    } catch (Exception ex) {
      logger.exception(ex, "ontick");
    }
  }

  private boolean isLeaderValid(LeaderInfo info) {
    if (System.currentTimeMillis() - info.getTime() > getLeaderExpirePeriod()) {
      return false;
    }
    logger.info("leader is valid . timeout {0}", (System.currentTimeMillis() - info.getTime()));
    return true;
  }

  // maintain leadership by updating timestamp
  private LeaderInfo updateLeader() {
    logger.debug("updateLeader (hb) started");
    if (leaderInfo.getLeaderId().equals(memberId)) {
      LeaderInfo newInfo =
          new LeaderInfo(leaderGroup, memberId, System.currentTimeMillis(), leaderInfo.getVersion() + 1);
      boolean updated = dataStore.replace(leaderGroup, leaderInfo,
          newInfo);
      if (!updated) {
        logger.info("updateLeader (hb) failed.another leader found");
        return dataStore.get(leaderGroup); // another leader found
      } else {
        logger.debug("updateLeader (hb) ok .{0}", newInfo);
        leaderInfo = newInfo;
      }
      return null;
    } else {
      throw new LeaderElectionException("not a leader to update the leadership");
    }
  }

  // if stale , update existing record, other wise insert if not exist
  private LeaderInfo becomeLeader(boolean stale) {
    logger.info("become a leader started, stale {0}", stale);
    LeaderInfo old;
    if (!stale) {
      LeaderInfo newInfo = new LeaderInfo(leaderGroup, memberId, System.currentTimeMillis(), 1);
      old = dataStore.putIfAbsent(leaderGroup, newInfo);
      if (old == null) {
        logger.info("leader elect success {0}", newInfo);
        return newInfo; // leader elected successfully.
      } else if (isLeaderValid(old)) {
        logger.info("leader elect failed. another leader found {0}", old);
        return old; // leader elect failed, but another leader found.
      }
    } else {
      old = leaderInfo;
    }

    if (isLeaderValid(old)) {
      logger.info("old leader still valid {0}", old);
      return null;
    }
    // stale leader found. replace it.
    LeaderInfo newInfo = new LeaderInfo(leaderGroup, memberId, System.currentTimeMillis(), old.getVersion() + 1);
    boolean updated = dataStore.replace(leaderGroup, old,
        newInfo);
    if (!updated) {
      logger.info("stale leader update failed , another leader found");
      return dataStore.get(leaderGroup); // another leader found.
    } else {
      logger.info("stale leader success {0}", newInfo);
      return newInfo; // leader elected sucessfully.
    }

  }

  // return only if leader is valid
  private LeaderInfo loadLeader() {
    LeaderInfo info = dataStore.get(leaderGroup);
    if (info != null && isLeaderValid(info)) {
      logger.info("load valid leader {0}", info);
      return info;
    }
    logger.info("valid leader not found");
    return null;
  }

  public MemberStatus getStatus() {
    return status;
  }

  public Optional<LeaderInfo> getLeaderInfo() {
    if (leaderInfo == null)
      return Optional.empty();
    else
      return Optional.of(leaderInfo);
  }

  public static final class LeaderInfo {

    private final String leaderGroup;
    private final String leaderId;
    private final long version;
    private final long time;

    public LeaderInfo(String leaderGroup, String leaderId, long time, long version) {
      this.leaderGroup = leaderGroup;
      this.leaderId = leaderId;
      this.version = version;
      this.time = time;
    }

    public String getLeaderGroup() {
      return leaderGroup;
    }

    public String getLeaderId() {
      return leaderId;
    }

    public long getVersion() {
      return version;
    }

    public long getTime() {
      return time;
    }

    @Override
    public int hashCode() {
      int hash = 3;
      hash = 53 * hash + (int) (this.version ^ (this.version >>> 32));
      return hash;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final LeaderInfo other = (LeaderInfo) obj;
      return this.version == other.version;
    }

    @Override
    public String toString() {
      return "LeaderInfo{" + "leaderGroup=" + leaderGroup + ", leaderId=" + leaderId + ", version=" + version
          + ", time=" + time + '}';
    }


  }

  public static interface Listener {

    void leaderChange(LeaderElection le, String leaderId);
  }
}
