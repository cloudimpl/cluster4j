/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.cloudimpl.cluster.network;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.questdb.network.EpollAccessor;
import io.questdb.network.EpollFacade;
import io.questdb.network.EpollFacadeImpl;
import io.questdb.network.EpollUtil;
import io.questdb.network.Net;
import io.questdb.network.NetworkError;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import java.util.function.Consumer;
import org.agrona.collections.Long2ObjectHashMap;
import reactor.core.publisher.Mono;

/**
 *
 * @author nuwansa
 */
public class XEpollEventLoop implements XEventLoop {

    private final EpollFacade epf;
    private final int capacity;
    private final long events;
    private long _ptr;
    private final long epollFd;
    private final Long2ObjectHashMap<XChannel> channels;
    private final ByteBuf recvBuf;
    private final XTimer[] timers;
    private int epollTimeout;

    public XEpollEventLoop(int capacity) {
        epf = EpollFacadeImpl.INSTANCE;
        this.capacity = capacity;
        this.events = Unsafe.calloc(EpollAccessor.SIZEOF_EVENT * (long) (capacity));
        this._ptr = this.events;// + EpollAccessor.SIZEOF_EVENT;
        this.epollFd = epf.epollCreate();
        this.channels = new Long2ObjectHashMap<>(capacity, 0.50f);
        this.recvBuf = PooledByteBufAllocator.DEFAULT.directBuffer(1024 * 1024,1024 * 1024);
        this.timers = new XTimer[4];
        this.epollTimeout = 1000;
    }

    @Override
    public void add(XChannel channel) {
        if (channel.type() == XChannel.TCP_CLIENT_SOCKET && !((XTcpClient) channel).isConnected()) {
            Unsafe.getUnsafe().putInt(events + EpollUtil.EVENTS_OFFSET, EpollAccessor.EPOLLIN | EpollAccessor.EPOLLOUT
                    | EpollUtil.EPOLLERR | EpollUtil.EPOLLHUP);
        } else {
            Unsafe.getUnsafe().putInt(events + EpollUtil.EVENTS_OFFSET, EpollAccessor.EPOLLIN
                    | EpollUtil.EPOLLERR | EpollUtil.EPOLLHUP | EpollUtil.EPOLLET);
        }

        Unsafe.getUnsafe().putLong(events + EpollUtil.DATA_OFFSET, channel.id());
        int ret = epf.epollCtl(this.epollFd, EpollAccessor.EPOLL_CTL_ADD, channel.id(), events);
        if (ret == 0) {
            channels.put(channel.id(), channel);
        } else {
            throw NetworkError.instance(Os.errno());
        }
    }

    @Override
    public void remove(XChannel channel) {
        int ret = epf.epollCtl(this.epollFd, EpollAccessor.EPOLL_CTL_DEL, channel.id(), events);
        if (ret == 0) {
            channels.remove(channel.id());
        } else {
            throw NetworkError.instance(Os.errno());
        }
    }

    @Override
    public void watchForWriteEvent(XChannel channel) {
        Unsafe.getUnsafe().putInt(events + EpollUtil.EVENTS_OFFSET, EpollAccessor.EPOLLOUT | EpollAccessor.EPOLLIN
                | EpollUtil.EPOLLERR | EpollUtil.EPOLLHUP | EpollUtil.EPOLLET);
        Unsafe.getUnsafe().putLong(events + EpollUtil.DATA_OFFSET, channel.id());
        int ret = epf.epollCtl(this.epollFd, EpollAccessor.EPOLL_CTL_MOD, channel.id(), events);
    }

    private boolean onReadEvent(XChannel channel) {
        // System.out.println("on read event");
        boolean alive = true;
        try {
            switch (channel.type()) {
                case XChannel.TCP_CLIENT_SOCKET: {
                    while (true) {
                        int ret = epf.getNetworkFacade().recv(channel.id(), recvBuf.memoryAddress(), (int) recvBuf.capacity());
                        //      System.out.println("read len : " + ret);
                        if (ret > 0) {
                            recvBuf.writerIndex(ret);
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
                        int ret = epf.getNetworkFacade().recv(channel.id(), recvBuf.memoryAddress(), (int) recvBuf.capacity());
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
                    XTcpClient client = new XTcpClient(id, channel.getCb());
                    client.setConnected(true);
                    add(client);
                    channel.getCb().onConnect(this, (XTcpClient) client);
                    break;
                }
                default:
                    break;
            }
        } catch (Exception ex) {
            return alive;
        }

        return alive;
    }

    private void onWriteEvent(XChannel channel) {
        System.out.println("on write event");
        if (channel.type() == XChannel.TCP_CLIENT_SOCKET) {
            XTcpClient client = (XTcpClient) channel;
            int ret = client.flush();
            if (ret == 0) {
                Unsafe.getUnsafe().putInt(events + EpollUtil.EVENTS_OFFSET, EpollAccessor.EPOLLIN
                        | EpollUtil.EPOLLERR | EpollUtil.EPOLLHUP | EpollUtil.EPOLLET);
                Unsafe.getUnsafe().putLong(events + EpollUtil.DATA_OFFSET, channel.id());
            }
            int ret1 = epf.epollCtl(this.epollFd, EpollAccessor.EPOLL_CTL_MOD, channel.id(), events);
            if (ret1 == 0) {
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
                this.epollTimeout = 0;
            }
            int n = epf.epollWait(this.epollFd, events, capacity, this.epollTimeout);
            //System.out.println("events received : " + n);
            triggerTimers();
            while (n > 0) {
                int event = getEvent();
                XChannel channel = channels.get(getData());
                boolean alive = true;
                // System.out.println("event :" + event);
                if ((event & EpollUtil.EPOLLERR) == EpollUtil.EPOLLERR || (event & EpollUtil.EPOLLHUP) == EpollUtil.EPOLLHUP) {
                    onErrorEvent(channel);
                } else {
                    if ((event & EpollAccessor.EPOLLIN) == EpollAccessor.EPOLLIN) {
                        alive = onReadEvent(channel);
                    }
                    if (alive && ((event & EpollAccessor.EPOLLOUT) == EpollAccessor.EPOLLOUT)) {
                        onWriteEvent(channel);
                    }
                }

                n--;
                _ptr += EpollAccessor.SIZEOF_EVENT;
            }
        }

    }

    private long getData() {
        return Unsafe.getUnsafe().getLong(_ptr + EpollUtil.DATA_OFFSET);
    }

    private int getEvent() {
        return Unsafe.getUnsafe().getInt(_ptr + EpollUtil.EVENTS_OFFSET);
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
                epollTimeout = minTimeout();
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

    @Override
    public Mono<AsyncTask> pushAsynTask(Consumer<AsyncTask> channelProvider) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
