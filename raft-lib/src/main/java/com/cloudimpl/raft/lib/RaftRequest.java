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

/**
 *
 * @author nuwansa
 */
public class RaftRequest {

    private final long term;
    private final String nodeId;
    private final Object msg;

    public RaftRequest(long term, String nodeId, Object msg) {
        this.term = term;
        this.nodeId = nodeId;
        this.msg = msg;
    }

    public long getTerm() {
        return term;
    }

    public String getNodeId() {
        return nodeId;
    }

    public Object getMsg() {
        return msg;
    }

}
