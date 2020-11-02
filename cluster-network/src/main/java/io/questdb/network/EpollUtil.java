/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.questdb.network;

import static io.questdb.network.EpollAccessor.getDataOffset;
import static io.questdb.network.EpollAccessor.getEPOLLET;
import static io.questdb.network.EpollAccessor.getEPOLLONESHOT;
import static io.questdb.network.EpollAccessor.getEventsOffset;

/**
 *
 * @author nuwansa
 */
public class EpollUtil {

    public static final short DATA_OFFSET;
    public static final short EVENTS_OFFSET;
    public static final int EPOLLONESHOT;
    public static final int EPOLLET;

    static {
        DATA_OFFSET = getDataOffset();
        EVENTS_OFFSET = getEventsOffset();

        EPOLLET = getEPOLLET();
        EPOLLONESHOT = getEPOLLONESHOT();

    }
    
    

    public static final int EPOLLIN = 0x001;
    public static final int EPOLLPRI = 0x002;
    public static final int EPOLLOUT = 0x004;
    public static final int EPOLLRDNORM = 0x040;
    public static final int EPOLLRDBAND = 0x080;
    public static final int EPOLLWRNORM = 0x100;
    public static final int EPOLLWRBAND = 0x200;
    public static final int EPOLLMSG = 0x400;
    public static final int EPOLLERR = 0x008;
    public static final int EPOLLHUP = 0x010;
    

}
