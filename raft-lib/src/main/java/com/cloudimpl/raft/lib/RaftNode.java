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
import com.cloudimpl.raft.lib.msg.ElectionResponse;
import com.cloudimpl.raft.lib.msg.HeartBeat;
import com.cloudimpl.raft.lib.msg.RaftPayload;
import com.cloudimpl.raft.lib.msg.VoteAck;

/**
 *
 * @author nuwansa
 */
public class RaftNode {

    public enum MemberStatus {
        FOLLOWER, CANDIDATE, LEADER
    }
    public enum Status {
        ACCEPTED, REJECTED
    }
    private long currentTerm;
    private long lastAckterm;
    private MemberStatus state;
    private final String nodeId;
    private String leaderId = "";
    
    public RaftNode(String nodeId) {
        this.nodeId = nodeId;
        this.currentTerm = 0;
        this.lastAckterm = 0;
        this.state = MemberStatus.FOLLOWER;
    }

    public MemberStatus getState()
    {
        return this.state;
    }
    
    
    public VoteAck requestVote(long requesterTerm)
    {
        if(currentTerm >= requesterTerm)
        {
            return createVoteAck(Status.REJECTED, "term is less than or equal for  what i have");
        }
        else if(lastAckterm != 0)
        {
            return createVoteAck(Status.REJECTED, "already voted");
        }
        else
        {
            this.lastAckterm = requesterTerm;
            return createVoteAck(Status.ACCEPTED, "vote accepted for termId "+requesterTerm);
        }
    }
    
    public void onHb(long requestTermId)
    {
        if(currentTerm <= requestTermId)
        {
            this.currentTerm = requestTermId;
            this.state = MemberStatus.FOLLOWER;
            this.lastAckterm = 0;
        }
    }
    
    protected ElectionReq createElection()
    {
        ElectionReq elReq =  new ElectionReq(++currentTerm,nodeId);
        state = MemberStatus.CANDIDATE;
        return elReq;
    }
    
    protected ElectionResponse onElectionReq(ElectionReq req)
    {
        if(req.getTermId() > currentTerm && this.lastAckterm == 0)
        {
            return new ElectionResponse(currentTerm, nodeId, Status.ACCEPTED, "accepted");
        }
        return new ElectionResponse(currentTerm, nodeId, Status.REJECTED, "invalid termid or already voted for term");
    }
    
    
    protected void onAppenEntry(RaftPayload payload)
    {
        if(payload.getTermId() == currentTerm && leaderId.equals(payload.getSenderId()))
        {
            
        }
    }
    
    private VoteAck createVoteAck(RaftNode.Status status,String reason)
    {
        return new VoteAck(currentTerm, nodeId, status, reason);
    }
}
