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
package com.cloudimpl.fass.node;

import reactor.core.publisher.Flux;

/**
 *
 * @author nuwansa
 */
public interface QueueManager {

    <T> Flux<T> createOrReplace(String queueName);

    public static QueueManager createQueueManager() {
        if (true) {
            return new AwsQueueManager();
        } else {
            return new LocalQueueManager();
        }
    }
}

class AwsQueueManager implements QueueManager {

    @Override
    public <T> Flux<T> createOrReplace(String queueName) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}

class LocalQueueManager implements QueueManager {

    @Override
    public <T> Flux<T> createOrReplace(String queueName) {
        return null;
    }

}

class Main {

    public static void main(String[] args) {
        QueueManager manager = QueueManager.createQueueManager();
        manager.createOrReplace("organization").doOnNext(Main::onMsg).doOnError(err->err.printStackTrace()).subscribe();

    }

    static void onMsg(Object msg) {
        
    }
}
