/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.cluster.network;

import io.questdb.std.Os;

/**
 *
 * @author nuwansa
 */
public class ErrorNo {
    public static final int EINPROGRESS;
    
    static{
        if(Os.type == Os.OSX || Os.type == Os.FREEBSD)
            EINPROGRESS = 36;
        else
            EINPROGRESS = 115;
    }
}
