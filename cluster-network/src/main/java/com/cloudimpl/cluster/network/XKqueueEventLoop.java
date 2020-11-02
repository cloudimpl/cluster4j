/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.cluster.network;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.questdb.network.KqueueAccessor;
import io.questdb.network.KqueueFacade;
import io.questdb.network.KqueueFacadeImpl;
import io.questdb.network.KqueueUtil;
import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import org.agrona.collections.Long2ObjectHashMap;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class XKqueueEventLoop implements XEventLoop {

    private final KqueueFacade kqf;
    private final int kqueueFd;
    private final long events;
    private long _ptr;
    private final Long2ObjectHashMap<XChannel> channels;
    private final ByteBuf recvBuf;
    private final XTimer[] timers;
    private int kqueueTimeout;
    private final int capacity;
    private final Queue<AsyncTask> controlQueue;
    public XKqueueEventLoop(int capacity) {
        this.kqf = KqueueFacadeImpl.INSTANCE;
        this.capacity = capacity;
        this.kqueueFd = kqf.kqueue();
        this.events = Unsafe.calloc(KqueueAccessor.SIZEOF_KEVENT * (long) (capacity));
        this.channels = new Long2ObjectHashMap<>(capacity, 0.50f);
        this.recvBuf = PooledByteBufAllocator.DEFAULT.directBuffer(1024 * 1024, 1024 * 1024);
        this._ptr = this.events;
        this.timers = new XTimer[4];
        this.kqueueTimeout = 1000;
        this.controlQueue = new ConcurrentLinkedQueue<>();
    }

    @Override
    public void add(XChannel channel) {
        if (channel.type() == XChannel.TCP_CLIENT_SOCKET && !((XTcpClient) channel).isConnected()) {
            Unsafe.getUnsafe().putInt(events + KqueueUtil.FILTER_OFFSET, KqueueUtil.EVFILT_WRITE);
        } else {
            Unsafe.getUnsafe().putInt(events + KqueueUtil.FILTER_OFFSET, KqueueUtil.EVFILT_READ);
        }

        Unsafe.getUnsafe().putLong(events + KqueueUtil.FD_OFFSET, channel.id());
        Unsafe.getUnsafe().putLong(events + KqueueUtil.FLAGS_OFFSET, KqueueUtil.EV_ADD | KqueueUtil.EV_ENABLE | KqueueUtil.EV_CLEAR);
        int ret = kqf.kevent(this.kqueueFd, events, 1, 0L, 0);
        if (ret == 0) {
            channels.put(channel.id(), channel);
        } else {
            throw NetworkError.instance(Os.errno());
        }
        channel.setEventLoop(this);
    }

    @Override
    public void remove(XChannel channel) {
        Unsafe.getUnsafe().putLong(events + KqueueUtil.FD_OFFSET, channel.id());
        Unsafe.getUnsafe().putLong(events + KqueueUtil.FLAGS_OFFSET, KqueueUtil.EV_DELETE);
        int ret = kqf.kevent(this.kqueueFd, events, 1, 0L, 0);
        if (ret == 0) {
            channels.remove(channel.id());
        } else {
            throw NetworkError.instance(Os.errno());
        }
    }

    @Override
    public void watchForWriteEvent(XChannel channel) {
        Unsafe.getUnsafe().putLong(events + KqueueUtil.FD_OFFSET, channel.id());
        Unsafe.getUnsafe().putInt(events + KqueueUtil.FILTER_OFFSET, KqueueUtil.EVFILT_WRITE);
        Unsafe.getUnsafe().putLong(events + KqueueUtil.FLAGS_OFFSET, KqueueUtil.EV_ADD | KqueueUtil.EV_ENABLE | KqueueUtil.EV_CLEAR);
        int ret = kqf.kevent(this.kqueueFd, events, 1, 0L, 0);
    }

    private boolean onReadEvent(XChannel channel) {
        // System.out.println("on read event");
        boolean alive = true;
        try {
            switch (channel.type()) {
                case XChannel.TCP_CLIENT_SOCKET: {
                    while (true) {
                        int ret = kqf.getNetworkFacade().recv(channel.id(), recvBuf.memoryAddress(), (int) recvBuf.capacity());
                        //      System.out.println("read len : " + ret);
                        if (ret > 0) {
                            recvBuf.writerIndex(ret);
                            recvBuf.readerIndex(0);
                            channel.getCb().onReadEvent(this, (XTcpClient) channel, recvBuf, ret);
                            recvBuf.clear();
                        } else if (ret == Net.EOTHERDISCONNECT) {
                            alive = false;
                            channels.remove(channel.id());
                            channel.close();
                            channel.getCb().onDisconnect(this, (XTcpClient) channel);
                            break;
                        } else if (ret == Net.ERETRY) {
                            break;
                        }
                    }

                    break;
                }
                case XChannel.UDP_CHANNEL_SOCKET: {
                    while (true) {
                        int ret = kqf.getNetworkFacade().recv(channel.id(), recvBuf.memoryAddress(), (int) recvBuf.capacity());
                        //       System.out.println("udp read len : " + ret);
                        if (ret > 0) {
                            channel.getCb().onReadEvent(this, (XUdpChannel) channel, recvBuf, ret);
                        } else if (ret <= 0) {
                            break;
                        }
                    }

                    break;
                }
                case XChannel.SERVER_SOCKET: {
                    long id = Net.accept(channel.id());
                    Net.setTcpNoDelay(id, true);
                    Net.configureNonBlocking(id);
                    Net.configureNoLinger(id);
                    XTcpClient client = new XTcpClient(id,channel.getCb());
                    client.setConnected(true);
                    client.setServerSocket(channel);
                    add(client);
                    channel.getCb().onConnect(this, (XTcpClient) client);
                    break;
                }
                default:
                    break;
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            return alive;
        }

        return alive;
    }

    private void onWriteEvent(XChannel channel) {
        System.out.println("on write event");
        if (channel.type() == XChannel.TCP_CLIENT_SOCKET) {
            XTcpClient client = (XTcpClient) channel;
            int ret = client.flush();
            int ret1 = 0;
            if(ret == 0)
            {
                ret1 = deleteEvent(channel.id(), KqueueUtil.EVFILT_WRITE);
            }
            int ret2 = addEvent(channel.id(), KqueueUtil.EVFILT_READ);
            if (ret1  == 0 && ret2 == 0) {
                if (client.isConnected() && ret == 0) {
                    channel.getCb().onWriteReady(this, (XTcpClient) channel);
                } else if(!client.isConnected()){
                    client.setConnected(true);
                    channel.getCb().onConnect(this, (XTcpClient) channel);
                }
            } else {
                channel.close();
                channels.remove(channel.id());
                channel.getCb().onDisconnect(this, (XTcpClient) channel);
            }

        }
    }

    private boolean onErrorEvent(XChannel channel) {
        System.out.println("on event error");
        try {
            if (channel.type() == XChannel.TCP_CLIENT_SOCKET) {
                channel.close();
                channels.remove(channel.id());
                channel.getCb().onDisconnect(this, (XTcpClient) channel);
            }
        } catch (Exception ex) {

        }
        return false;
    }

    @Override
    public void run(boolean polling) {
        while (true) {
            _ptr = events;
            if (polling) {
                //      this.epollTimeout = 0;
            }
            int n = kqf.kevent(kqueueFd, 0L, 0, events, capacity);
            //System.out.println("events received : " + n);
            pollControlQueue();
            triggerTimers();
            while (n > 0) {
                int event = getEvent();
                long fd = getData();
                XChannel channel = channels.get(fd);
                boolean alive = true;
                // System.out.println("event :" + event);
                if ((getFlag() & KqueueUtil.EV_EOF) == KqueueUtil.EV_EOF) {
                    onErrorEvent(channel);
                } else {
                    if (event == KqueueUtil.EVFILT_READ) {
                        alive = onReadEvent(channel);
                    }
                    if (alive && (event == KqueueUtil.EVFILT_WRITE)) {
                        onWriteEvent(channel);
                    }
                }

                n--;
                _ptr += KqueueUtil.SIZEOF_KEVENT;
            }
        }

    }

    private long getData() {
        return Unsafe.getUnsafe().getLong(_ptr + KqueueUtil.FD_OFFSET);
    }

    private int getEvent() {
        return Unsafe.getUnsafe().getShort(_ptr + KqueueUtil.FILTER_OFFSET);
    }

    private int getFlag() {
        return Unsafe.getUnsafe().getShort(_ptr + KqueueUtil.FLAGS_OFFSET);
    }

    private void triggerTimers() {
        long time = System.nanoTime();
        int i = 0;
        while (i < timers.length) {
            XTimer timer = timers[i];
            if (timer != null) {
                if (time - timer.getLastTrigger() >= timer.getMicro() * 1000) {
                    try {
                        timer.updateTriggerTime(time);
                        timer.getCb().onTimer(timer);
                    } catch (Exception ex) {

                    }
                }
            }
            i++;
        }
    }

    @Override
    public XTimer createTimer(long micro, XTimerCallback cb) {
        int i = 0;

        while (i < timers.length) {
            if (timers[i] == null) {
                timers[i] = new XTimer(micro, i, cb);
                kqueueTimeout = minTimeout();
                break;
            }
            i++;
        }
        if (i == timers.length) {
            return null;
        }

        return timers[i];
    }

    private int minTimeout() {
        int i = 0;
        int millis = 1000;
        while (i < timers.length) {
            if (timers[i] != null) {
                millis = (int) Math.min(millis, timers[i].getMicro() / 1000);
            } else {
                break;
            }
            i++;
        }
        return millis;
    }

    private int addEvent(long fd, short event) {
        Unsafe.getUnsafe().putLong(events + KqueueUtil.FD_OFFSET, fd);
        Unsafe.getUnsafe().putInt(events + KqueueUtil.FILTER_OFFSET, event);
        Unsafe.getUnsafe().putLong(events + KqueueUtil.FLAGS_OFFSET, KqueueUtil.EV_ADD | KqueueUtil.EV_ENABLE | KqueueUtil.EV_CLEAR);
        return kqf.kevent(this.kqueueFd, events, 1, 0L, 0);
    }
    
    private int deleteEvent(long fd, short event) {
        Unsafe.getUnsafe().putLong(events + KqueueUtil.FD_OFFSET, fd);
        Unsafe.getUnsafe().putInt(events + KqueueUtil.FILTER_OFFSET, event);
        Unsafe.getUnsafe().putLong(events + KqueueUtil.FLAGS_OFFSET, KqueueUtil.EV_DELETE);
        return kqf.kevent(this.kqueueFd, events, 1, 0L, 0);
    }
    
    
    private void pollControlQueue()
    {
        if(!controlQueue.isEmpty())
        {
            AsyncTask sink;
            while((sink = controlQueue.poll()) != null)
            {
                try
                {
                    sink.setLoop(this);
                    sink.getChannelProvider().accept(sink);
                    sink.success();
                }catch(Exception ex)
                {
                    sink.error(ex);
                }
                
            }
        }
    }

    @Override
    public Mono<AsyncTask> pushAsynTask(Consumer<AsyncTask> channelProvider) {
        return new AsyncTask(channelProvider, controlQueue,this).asMono();
    }

}
