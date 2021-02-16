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
package test;

import com.cloudimpl.cluster.common.FluxProcessor;

/**
 *
 * @author nuwan
 * @param <T>
 */
public class CompactionManager<T extends QueryBlock> {
    private final CompactionWorker[] workers;
    private final CompactionWorkerProvider provider;
    private final FluxProcessor<T> itemProcessor = new FluxProcessor<>();
    private final ColumnIndex idx;
    public CompactionManager(int levelCount,ColumnIndex idx,CompactionWorkerProvider provider) {
        workers = new CompactionWorker[levelCount];
        this.idx = idx;
        this.provider = provider;
        init();
    }
    
    public  void submit(int level,T queryBlock)
    {
        itemProcessor.add(queryBlock);
    }
    
    private void init()
    {
 
        
        int i = 0;
        while(i < workers.length)
        {
            final CompactionWorker worker = provider.apply(i);
            workers[i] = worker;
            itemProcessor.flux().publishOn(idx.getCompactionScheduler())
                    
                    .doOnNext(q->worker.submit(q))
                    .doOnError(thr->thr.printStackTrace())
                    .subscribe();
            i++;
        }
    }
}
