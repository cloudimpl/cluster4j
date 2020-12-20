/*
 * To change this license header, choose License Headers in Project Properties. To change this template file, choose
 * Tools | Templates and open the template in the editor.
 */
package com.cloudimpl.cluster4j.le;

import com.cloudimpl.cluster.common.FluxMap;
import com.cloudimpl.cluster4j.common.Pair;
import com.cloudimpl.cluster4j.core.logger.ILogger;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
    private Listener listener;
    private int count = 0;
    public MemberStatus status;
    private LeaderInfo leaderInfo;

    private final ScheduledExecutorService schedular;

    private final Set<String> members;
    private final Queue<Object> pendingEvents;
    private final FluxMap<String, LeaderInfo> leaderMap;
    private long leaderExpirePeriod = 10000;
    private final ILogger logger;
    private ScheduledFuture<?> timerHnd;

    protected LeaderElection(String leaderGroup, String memberId, Map<String, LeaderInfo> dataStore,
            long leaderExpirePeriod,
            FluxMap<String, LeaderInfo> leaderMap, ILogger logger) {
        this.logger = logger.createSubLogger("LeaderElection", leaderGroup + "-" + memberId);
        this.dataStore = dataStore;
        this.leaderMap = leaderMap;
        this.memberId = memberId;
        this.leaderInfo = null;
        this.leaderGroup = leaderGroup;
        this.status = MemberStatus.FOLLOWER;
        this.schedular = Executors.newSingleThreadScheduledExecutor();
        this.members = new ConcurrentSkipListSet<>();
        this.members.add(memberId);
        this.pendingEvents = new ConcurrentLinkedQueue<>();
        this.leaderExpirePeriod = leaderExpirePeriod;
    }

    public void run(Listener listener) {
        this.listener = listener;
        timerHnd = schedular.scheduleAtFixedRate(this::onTick, 5, 1, TimeUnit.SECONDS);
    }

    public void reset() {
        if (timerHnd != null) {
            timerHnd.cancel(false);
        }
        Random r = new Random(System.currentTimeMillis());
        int initialDelay = 10000 + r.nextInt(4000); //TODO improve this
        timerHnd = schedular.scheduleAtFixedRate(this::onTick, initialDelay, 1000, TimeUnit.MILLISECONDS);
        logger.info("reset with initial delay {0}", initialDelay);
    }

    public void close() {
        if (timerHnd != null) {
            timerHnd.cancel(true);
            timerHnd = null;
        }
    }

    public String getMemberId() {
        return memberId;
    }

    protected void onLeaderChange(LeaderInfo info) {
        logger.info("leader change called {0}", info);
        if (members.contains(info.getLeaderId())) {
            logger.info("leader change callback called {0}", info);
            listener.leaderChange(this, info.getLeaderId());

            if (leaderMap != null) {
                leaderMap.put(leaderGroup, info);
            }
        }
    }

    public void addLeaderInfo(LeaderInfo info) {
        logger.info("add leader info {0}", info);
        pendingEvents.add(info);
    }

    public void addMember(String memberId) {
        logger.info("add member {0}", memberId);
        pendingEvents.add(new MemberEvent(memberId, true));
    }

    public void removeMember(String memberId) {
        logger.info("remove member {0}", memberId);
        pendingEvents.add(new MemberEvent(memberId, false));
    }

    private boolean isMember(String memberId) {
        return members.contains(memberId);
    }

    public long getLeaderExpirePeriod() {
        return leaderExpirePeriod;
    }

    private boolean  processQueue() {
        Object event;
        while ((event = pendingEvents.poll()) != null) {
            if (event instanceof MemberEvent) {
                MemberEvent memberEvent = (MemberEvent) event;
                boolean ok;
                if (memberEvent.isAdded()) {
                    ok = members.add(memberEvent.getMemberId());
                } else {
                    ok = members.remove(memberEvent.getMemberId());
                }
                if (leaderInfo != null) {
                    if (memberEvent.isAdded() && ok) {
                        if (leaderInfo.getLeaderId().equals(memberEvent.getMemberId())) {
                            onLeaderChange(leaderInfo);
                        }
                    } else if (ok) {
                        if (leaderInfo.getLeaderId().equals(memberEvent.getMemberId())) {
                            reset();
                            status = MemberStatus.CANDIDATE;
                            return false;
                        }
                    }
                }
            } else if (event instanceof LeaderInfo) {
                updateLeaderInfo((LeaderInfo) event);
            }

        }
        return true;
    }

    protected void onTick() {
        logger.debug("tick");
        try {
            if(!processQueue())
                return;
            LeaderInfo newInfo = null;
            switch (status) {
                case FOLLOWER:
                    logger.debug("follower leaderInfo {0}", leaderInfo);
                    if (leaderInfo == null) {
                        newInfo = loadLeader(); // load the leader here
                        if (newInfo != null) {
                            onLeaderChange(newInfo);
                        } else {
                            reset();
                            status = MemberStatus.CANDIDATE; // no leader found , become a candidate
                        }
                    } else if (!isMember(leaderInfo.leaderId)) {
                        logger.info("leader {0} is not in the member list and leader outdated", leaderInfo.leaderId);
                        reset();
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
                            reset();
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
        long time = System.currentTimeMillis();
        if (time - info.getTime() > getLeaderExpirePeriod()) {
            logger.info("leader is invalid . timeout {0}", (time - info.getTime()));
            return false;
        }
        logger.info("leader is valid . timeout {0}", (time - info.getTime()));
        return true;
    }

    // maintain leadership by updating timestamp
    private LeaderInfo updateLeader() {
        logger.debug("updateLeader (hb) started");
        if (leaderInfo.getLeaderId().equals(memberId)) {
            LeaderInfo newInfo
                    = new LeaderInfo(leaderGroup, memberId, System.currentTimeMillis(), leaderInfo.getVersion() + 1);
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

    private boolean updateLeaderInfo(LeaderInfo info) {

        if (leaderInfo == null || info.getVersion() > leaderInfo.getVersion()) {
            this.leaderInfo = info;
            logger.info("leader info updated: {0}", info);
            if (!leaderInfo.getLeaderId().equals(this.memberId)) {
                this.status = MemberStatus.FOLLOWER;
                logger.info("new leader found and becoming follower");
            }
            onLeaderChange(info);
            return true;
        }
        logger.info("leader info not updated. current {0} and apply {1}", this.leaderInfo, info);
        return false;
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
            return old;
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
        logger.info("valid leader not found- info : {0}", info);
        return null;
    }

    public MemberStatus getStatus() {
        return status;
    }

    public Optional<LeaderInfo> getLeaderInfo() {
        if (leaderInfo == null) {
            return Optional.empty();
        } else {
            return Optional.of(leaderInfo);
        }
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

    public static final class MemberEvent {

        private final String memberId;
        private final boolean added;

        public MemberEvent(String memberId, boolean added) {
            this.memberId = memberId;
            this.added = added;
        }

        public boolean isAdded() {
            return added;
        }

        public String getMemberId() {
            return memberId;
        }

    }

}
