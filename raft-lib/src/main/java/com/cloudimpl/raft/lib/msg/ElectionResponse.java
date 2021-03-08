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
package com.cloudimpl.raft.lib.msg;

import com.cloudimpl.raft.lib.RaftNode;

/**
 *
 * @author nuwan
 */
public class ElectionResponse extends RaftMsg{
    private final RaftNode.Status status;
    private final String reason;
    public ElectionResponse(long termId, String senderId,RaftNode.Status status,String reason) {
        super(termId, senderId);
        this.status = status;
        this.reason = reason;
    }

    public String getReason() {
        return reason;
    }

    public RaftNode.Status getStatus() {
        return status;
    }
    
    
}
