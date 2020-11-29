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
package com.cloudimpl.cluster4j.le;

/**
 *
 * @author nuwansa
 */
public class LeaderInfoResponse {
    private final LeaderElection.LeaderInfo info;
    public LeaderInfoResponse(LeaderElection.LeaderInfo info) {
        this.info = info;
    }

    public String getLeaderGroup() {
        return info.getLeaderGroup();
    }

    public String getLeaderId() {
        return info.getLeaderId();
    }
    
    public LeaderElection.LeaderInfo getLeaderInfo()
    {
        return this.info;
    }
}
