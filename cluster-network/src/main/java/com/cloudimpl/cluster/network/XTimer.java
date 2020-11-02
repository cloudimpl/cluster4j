/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.cluster.network;

/**
 *
 * @author nuwansa
 */
public class XTimer {
    private final long micros;
    private final int index;
    private final XTimerCallback cb;
    private  long lastTrigger;
    public XTimer(long micros, int index,XTimerCallback cb) {
        this.micros = micros;
        this.index = index;
        this.cb = cb;
        this.lastTrigger = System.currentTimeMillis();
    }

    public int getIndex() {
        return index;
    }

    public long getMicro() {
        return micros;
    }

    public long getLastTrigger() {
        return lastTrigger;
    }

    public XTimerCallback getCb() {
        return cb;
    }
    
    
    protected void updateTriggerTime(long time)
    {
        this.lastTrigger = time;
    }
    
}
