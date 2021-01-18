/*
 * Copyright 2021 nuwansa.
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
package com.cloudimpl.jgroup;

/**
 *
 * @author nuwansa
 */
public class ConnectMsg {
    private final String me;
    private final String connect;
    private final boolean ctrl;

    public ConnectMsg(String me, String connect, boolean ctrl) {
        this.me = me;
        this.connect = connect;
        this.ctrl = ctrl;
    }

    public boolean isCtrl() {
        return ctrl;
    }
    
    public String getMe() {
        return me;
    }

    public String getConnect() {
        return connect;
    }

    @Override
    public String toString() {
        return "ConnectMsg{" + "me=" + me + ", connect=" + connect + ", ctrl=" + ctrl + '}';
    }

    
    
    
}
