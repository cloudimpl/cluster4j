/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.questdb.network;

import static io.questdb.network.KqueueAccessor.getDataOffset;
import static io.questdb.network.KqueueAccessor.getEvAdd;
import static io.questdb.network.KqueueAccessor.getEvOneshot;
import static io.questdb.network.KqueueAccessor.getEvfiltRead;
import static io.questdb.network.KqueueAccessor.getEvfiltWrite;
import static io.questdb.network.KqueueAccessor.getFdOffset;
import static io.questdb.network.KqueueAccessor.getFilterOffset;
import static io.questdb.network.KqueueAccessor.getFlagsOffset;
import static io.questdb.network.KqueueAccessor.getSizeofKevent;

/**
 *
 * @author nuwansa
 */
public class KqueueUtil {

    public static final short EVFILT_READ;
    public static final short SIZEOF_KEVENT;
    public static final int EV_EOF = -32751;
    public static final short EV_ONESHOT;
    public static final short EVFILT_WRITE;
    public static final short FD_OFFSET;
    public static final short FILTER_OFFSET;
    public static final short FLAGS_OFFSET;
    public static final short DATA_OFFSET;
    public static final short EV_ADD;
    public static final short EV_DELETE	 = 0x0002;
    public static final short EV_ENABLE  = 0x0004;
    public static final short EV_CLEAR  = 0x0020;

    static {
        EVFILT_READ = getEvfiltRead();
        EVFILT_WRITE = getEvfiltWrite();
        SIZEOF_KEVENT = getSizeofKevent();
        FD_OFFSET = getFdOffset();
        FILTER_OFFSET = getFilterOffset();
        FLAGS_OFFSET = getFlagsOffset();
        DATA_OFFSET = getDataOffset();
        EV_ADD = getEvAdd();
        EV_ONESHOT = getEvOneshot();
    }
}
