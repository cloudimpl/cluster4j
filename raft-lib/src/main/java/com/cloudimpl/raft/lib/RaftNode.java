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
package com.cloudimpl.raft.lib;

import com.cloudimpl.raft.lib.msg.ElectionReq;
import com.cloudimpl.raft.lib.msg.HeartBeat;

/**
 *
 * @author nuwansa
 */
public class RaftNode {

    public enum State {
        FOLLOWER, CANDIDATE, LEADER
    }
    private long currentTerm;
    private String lastVoteFor;
    private State state;
    private final String nodeId;
    private String leaderId = "";
    private final ElectionManager electionManager;
    private  int electionTicks = 0;

    public RaftNode(String nodeId, ElectionManager electionManager) {
        this.electionManager = electionManager;
        this.nodeId = nodeId;
        this.currentTerm = 0;
        this.lastVoteFor = null;
        this.state = State.FOLLOWER;
    }

    public RaftRequest onTick()
    {
        this.electionTicks++;
        if(this.electionTicks > electionManager.getElectionTicks())
        {
            this.currentTerm++;
            this.lastVoteFor = nodeId;
            this.state = State.CANDIDATE;
            return new RaftRequest(currentTerm, nodeId, new ElectionReq());
        }
        return null;
    }
    
    public RaftResponse onMsg(RaftRequest req) {
        
        this.electionTicks = 0;
        
        if (req.getMsg() instanceof HeartBeat) {
            return onHb(req);
        }
        return null;
    }

    private RaftResponse onHb(RaftRequest req) {
        if (req.getTerm() > currentTerm) {
            currentTerm = req.getTerm();
            state = State.FOLLOWER;
            this.lastVoteFor = null;
        }
        return new RaftResponse(currentTerm, nodeId, req);
    }
}
