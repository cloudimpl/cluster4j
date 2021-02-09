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

import com.aitusoftware.babl.config.BablConfig;
import com.aitusoftware.babl.config.PerformanceMode;
import com.aitusoftware.babl.config.PropertiesLoader;
import com.aitusoftware.babl.user.Application;
import com.aitusoftware.babl.user.ContentType;
import com.aitusoftware.babl.websocket.BablServer;
import com.aitusoftware.babl.websocket.DisconnectReason;
import com.aitusoftware.babl.websocket.Session;
import com.aitusoftware.babl.websocket.SessionContainers;
import java.nio.file.Paths;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.ShutdownSignalBarrier;

/**
 *
 * @author nuwan
 */
public class WebSocketTest implements Application{

    public static void main(String[] args) {
        launchBabl();
    }
    private static void launchBabl() {
        final BablConfig config = new BablConfig();
        config.performanceConfig().performanceMode(PerformanceMode.DEVELOPMENT);
        config.applicationConfig().application(new WebSocketTest());
        try (SessionContainers containers = BablServer.launch(config)) {
            containers.start();
            new ShutdownSignalBarrier().await();
        }
    }

    @Override
    public int onSessionConnected(Session sn) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int onSessionDisconnected(Session sn, DisconnectReason dr) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public int onSessionMessage(Session sn, ContentType ct, DirectBuffer db, int i, int i1) {
        int k = i;
       while (k < i1)
       {
           k++;
           System.out.println((char)db.getByte(k));
       }
        //throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        sn.send(ContentType.TEXT, db, i, i1);
        return 0;
    }

}
