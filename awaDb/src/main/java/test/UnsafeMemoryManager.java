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

import io.questdb.std.Files;
import io.questdb.std.FilesFacadeImpl;
import io.questdb.std.Os;
import io.questdb.std.Unsafe;
import java.nio.channels.FileChannel;
import java.nio.file.Path;

/**
 *
 * @author nuwan
 */
public class UnsafeMemoryManager implements MemoryManager {

    @Override
    public OffHeapMemory allocateNative(long size) {
        long addr =  Unsafe.getUnsafe().allocateMemory(size);
        UnsafeNativeMemory seg = new UnsafeNativeMemory(addr,size);
        return seg;
    }

    @Override
    public synchronized OffHeapMemory mapFromPath(Path path, long bytesOffset, long bytesSize, FileChannel.MapMode mapMode) {
        try (io.questdb.std.str.Path p = new io.questdb.std.str.Path().of(path.toFile().getAbsolutePath())) {
             long fd;
             int mode = -1;
            if (mapMode == FileChannel.MapMode.READ_WRITE) {
                fd = FilesFacadeImpl.INSTANCE.openRW(p);
                mode = Files.MAP_RW;
            }else if(mapMode == FileChannel.MapMode.READ_ONLY)
            {
                fd = FilesFacadeImpl.INSTANCE.openRO(p);
                mode = Files.MAP_RO;
            }else
            {
                throw new RuntimeException("unsupported filemode: "+mapMode);
            }
         //   System.err.println("create fd: "+fd + " for path : "+path + "open files : "+FilesFacadeImpl.INSTANCE.getOpenFileCount());
            if(fd == -1)
            {
                System.out.println("error num: "+Os.errno());
                return mapFromPath(path, bytesOffset, bytesSize, mapMode);
               // throw new RuntimeException("error : "+Os.errno());
            }
            long actualSize = FilesFacadeImpl.INSTANCE.length(fd);
            FilesFacadeImpl.INSTANCE.truncate(fd, Math.max(actualSize, bytesOffset + bytesSize));
            long addr = FilesFacadeImpl.INSTANCE.mmap(fd, bytesSize, bytesOffset, mode);
            FilesFacadeImpl.INSTANCE.close(fd);
            return new UnsafeNativeMemory(addr, bytesSize);
        }

    }

}
