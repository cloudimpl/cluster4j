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
package com.cloudimpl.msg.lib;

import org.agrona.collections.Object2ObjectHashMap;
import org.green.jelly.JsonNumber;
import org.green.jelly.JsonParser;
import org.green.jelly.JsonParserListener;

/**
 *
 * @author nuwan
 */
public class XJsonParser {

    private final JsonParser parser;
    private final JsonListener listener;
    private int level;
    private ZCharSequence member;
    private ZCharSequence value;
    private final Listener msgListener;
    private Object2ObjectHashMap<ZCharSequence, JsonMsg> msgCache = new Object2ObjectHashMap<>();

    private JsonMsg currentMsg = null;
    public XJsonParser(Listener msgListener) {
        listener = new JsonListener(this);
        parser = new JsonParser();
        this.level = 0;
        parser.setListener(listener);
        this.msgListener = msgListener;
        this.member = new ZCharSequence(100);
        this.value = new ZCharSequence(100);
    }

    public void parse(CharSequence jsonStream) {
        parser.parse(jsonStream);
    }

    public void reset(){
        parser.reset();
    }
    
    private void onMsgStart() {

    }

    private void onMsgEnd() {
        if (level == 0) {
            this.msgListener.onMsg(currentMsg);
            currentMsg = null;
        }

    }

    private void onError() {

    }

    private void increaseLevel() {
        this.level++;
    }

    private void decreaseLevel() {
        this.level--;
    }

    private void onMember(CharSequence member) {
        if (level == 1) {
            this.member.append(member);
        }
    }

    private void onMemberValue(CharSequence value) {
        if (CharSequence.compare(this.member,"_type") == 0) {
            initMsg(value);
        } else if (!this.member.isEmpty() && this.currentMsg != null) {
            this.currentMsg.set(this.member, value);
        }
        this.member.reset();
    }

    private void onMemberValue(JsonNumber value) {

        if (!this.member.isEmpty() && this.currentMsg != null) {
            this.currentMsg.set(this.member, value);
        }
        this.member.reset();
    }

    private JsonMsg initMsg(CharSequence msgType) {
        value.append(msgType);
        currentMsg = msgCache.get(value);
        if (currentMsg == null) {
            currentMsg = JsonMsg.create(value);
            msgCache.put(new ZCharSequence(msgType), currentMsg);
        }
        currentMsg.reset();
        value.reset();
        return currentMsg;
    }

    public static interface Listener {

        void onMsg(JsonMsg msg);
    }

    public static final class JsonListener implements JsonParserListener {

        private final XJsonParser parser;

        public JsonListener(XJsonParser parser) {
            this.parser = parser;
        }

        @Override
        public void onJsonStarted() {

        }

        @Override
        public void onError(String error, int position) {
            parser.onError();
        }

        @Override
        public void onJsonEnded() {

        }

        @Override
        public boolean onObjectStarted() {
            parser.increaseLevel();
            parser.onMsgStart();
            return true;
        }

        @Override
        public boolean onObjectMember(CharSequence name) {
            parser.onMember(name);
            return true;
        }

        @Override
        public boolean onObjectEnded() {
            parser.decreaseLevel();
            parser.onMsgEnd();
            return true;
        }

        @Override
        public boolean onArrayStarted() {
            return true;
        }

        @Override
        public boolean onArrayEnded() {
            return true;
        }

        @Override
        public boolean onStringValue(CharSequence data) {
            parser.onMemberValue(data);
            return true;
        }

        @Override
        public boolean onNumberValue(JsonNumber number) {
            parser.onMemberValue(number);
            return true;
        }

        @Override
        public boolean onTrueValue() {
            return true;
        }

        @Override
        public boolean onFalseValue() {
            return true;
        }

        @Override
        public boolean onNullValue() {
            return true;
        }

    }
}
