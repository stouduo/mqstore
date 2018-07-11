package io.openmessaging.model;

import io.openmessaging.config.Config;
import sun.misc.Cleaner;
import sun.misc.Unsafe;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MappedFile {

    // 文件名
    private String fileName;

    // 文件所在目录路径
    private String fileDirPath;

    // 文件对象
    private File file;

    private MappedByteBuffer mappedByteBuffer;
    private long address=0;
    private FileChannel fileChannel;
    private boolean boundSuccess = false;
    private int fileSize;

    // 最大的脏数据量,系统必须触发一次强制刷
    private long fileFlushSize = Config.fileFlushSize;

    // 最大的刷间隔,系统必须触发一次强制刷
    private long fileFlushInterval = Config.fileFlushInterval;

    // 上一次刷数据
    private long lastFlushFileSize = 0;

    private AtomicLong writeSize = new AtomicLong(0);
    private static ScheduledExecutorService sioWorker = Executors.newSingleThreadScheduledExecutor((r) -> {
        Thread thread = new Thread(r);
        thread.setName("flush-to-disk");
        thread.setDaemon(true);
        return thread;
    });

    public MappedFile setFileFlushSize(int size) {
        this.fileFlushSize = size;
        return this;
    }

    public MappedFile(String fileName, String fileDirPath, int fileSize, boolean async) {
        this.fileName = fileName;
        this.fileDirPath = fileDirPath;
        this.file = new File(fileDirPath);
        if (!file.exists()) file.mkdirs();
        this.file = new File(fileDirPath + File.separator + fileName);
        this.fileSize = fileSize;
        if (async) {
            sioWorker.scheduleAtFixedRate(this::flush, 0, 500, TimeUnit.MILLISECONDS);
//            this.ioWorker = Executors.newFixedThreadPool(1, (r) -> {
//                Thread thread = new Thread(r);
//                thread.setDaemon(true);
//                return thread;
//            });
//            ioWorker.execute(this::flush);
        }
        try {
            file.delete();
            file.createNewFile();
            try {
                this.fileChannel = new RandomAccessFile(file, "rw").getChannel();
            } catch (Exception e) {
                e.printStackTrace();
                this.boundSuccess = false;
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        boundChannelToByteBuffer();
    }

    private void flush() {
        if (writeSize.get() - lastFlushFileSize != 0) {
            mappedByteBuffer.force();
            lastFlushFileSize = writeSize.get();
            System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS").format(new Date()) + "--" + Thread.currentThread().getName() + "-" + Thread.currentThread().getId() + ": flush " + fileName + " to disk:" + (writeSize.get() / 1024 / 1024) + "M");
        }
    }
//    private void flush() {
//        while (true) {
//            if (lastFlushFileSize == fileSize) {
//                clean();
//                break;
//            }
//            if (writeSize.get() - lastFlushFileSize != 0) {
//                mappedByteBuffer.force();
//                lastFlushFileSize = writeSize.get();
//                System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS").format(new Date()) + "--" + Thread.currentThread().getName() + "-" + Thread.currentThread().getId() + ": flush " + fileName + " to disk:" + (writeSize.get() / 1024 / 1024) + "M");
//            }
//            try {
//                Thread.sleep(fileFlushInterval);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }
//    }

    /**
     * 内存映照文件绑定
     *
     * @return
     */
    public synchronized boolean boundChannelToByteBuffer() {
        try {
            this.mappedByteBuffer = this.fileChannel
                    .map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
        } catch (IOException e) {
            e.printStackTrace();
            this.boundSuccess = false;
            return false;
        }

        this.boundSuccess = true;
        return true;
    }


    public synchronized ByteBuffer read(int offset, int size) {
        if (!boundSuccess) boundChannelToByteBuffer();
        int position = mappedByteBuffer.position();
        mappedByteBuffer.position(offset);
        ByteBuffer byteBuffer = mappedByteBuffer.slice();
        mappedByteBuffer.position(position);
        byteBuffer.limit(size);
        return byteBuffer;
    }

    public synchronized ByteBuffer readByChannel(int offset, int size) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(size);
        try {
            fileChannel.read(byteBuffer, offset);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byteBuffer.flip();
        return byteBuffer;
    }

    public synchronized int readByChannel(int offset) {
        return readByChannel(offset, 4).getInt();
    }


    /**
     * 在文件末尾追加数据
     *
     * @param data
     * @return
     * @throws Exception
     */
    public boolean appendData(byte[] data) {
        return appendData(data, 0, data.length);
    }

    public boolean appendData(ByteBuffer byteBuffer) {
        writeSize.getAndAdd(byteBuffer.position());
        if (writeSize.get() > fileSize) {   // 如果写入data会超出文件大小限制，不写入
//            flush();
            writeSize.getAndAdd(-byteBuffer.position());
            System.out.println("File="
                    + file.toURI().toString()
                    + " is written full.");
            System.out.println("already write data length:"
                    + writeSize
                    + ", max file size=" + fileSize);
            return false;
        }
        byteBuffer.flip();
        mappedByteBuffer.put(byteBuffer);
        byteBuffer.clear();
        return true;
    }

    public boolean appendData(byte[] data, int offset, int length) {
        writeSize.getAndAdd(length);
        if (writeSize.get() > fileSize) {   // 如果写入data会超出文件大小限制，不写入
//            flush();
            writeSize.getAndAdd(-length);
            System.out.println("File="
                    + file.toURI().toString()
                    + " is written full.");
            System.out.println("already write data length:"
                    + writeSize
                    + ", max file size=" + fileSize);
            return false;
        }
        this.mappedByteBuffer.put(data, offset, length);
//        flush();
        return true;
    }

    public MappedFile appendData(int offset, byte[] data) {
        writeSize.getAndAdd(data.length);
        if (writeSize.get() > fileSize) {   // 如果写入data会超出文件大小限制，不写入
            writeSize.getAndAdd(-data.length);
            System.out.println("File="
                    + file.toURI().toString()
                    + " is written full.");
            System.out.println("already write data length:"
                    + writeSize
                    + ", max file size=" + fileSize);
            return this;
        }
        for (int i = offset; i < offset + data.length; i++) {
            mappedByteBuffer.put(i, data[i - offset]);
        }
        return this;
    }

    public MappedFile writeInt(int offset, int value) {
        writeSize.getAndAdd(4);
        mappedByteBuffer.putInt(offset, value);
//        flush();
        return this;
    }

    public MappedFile writeLong(int offset, long value) {
        writeSize.getAndAdd(8);
        mappedByteBuffer.putLong(offset, value);
//        flush();
        return this;
    }

    public MappedFile writeInt(int value) {
        writeSize.getAndAdd(4);
        mappedByteBuffer.putInt(value);
//        flush();
        return this;
    }

    public MappedFile writeLong(long value) {
        writeSize.getAndAdd(8);
        mappedByteBuffer.putLong(value);
//        flush();
        return this;
    }

    public void clean() {
        AccessController.doPrivileged((PrivilegedAction) () -> {
            try {
                Method getCleanerMethod = mappedByteBuffer.getClass().getMethod("cleaner", new Class[0]);
                getCleanerMethod.setAccessible(true);
                Cleaner cleaner = (Cleaner)
                        getCleanerMethod.invoke(mappedByteBuffer, new Object[0]);
                cleaner.clean();
                System.out.println("clean bytebuff success");
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        });
    }

    public int getInt(int offset) {
        if (!boundSuccess) boundChannelToByteBuffer();
        return mappedByteBuffer.getInt(offset);
    }

    public long getLong(int offset) {
        if (!boundSuccess) boundChannelToByteBuffer();
        return mappedByteBuffer.getLong(offset);
    }

//    public void flush(int writePosition) {
////        System.out.println("flush mqstore to disk:" + writePosition);
//        this.mappedByteBuffer.force();
//        this.lastFlushTime = System.currentTimeMillis();
//        this.lastFlushFilePosition = writePosition;
//    }


    public String getFileName() {
        return fileName;
    }

    public String getFileDirPath() {
        return fileDirPath;
    }

    public boolean isBundSuccess() {
        return boundSuccess;
    }

    public File getFile() {
        return file;
    }

    public MappedByteBuffer getMappedByteBuffer() {
        return mappedByteBuffer;
    }

    public MappedFile setFileFlushInterval(int fileFlushInterval) {
        this.fileFlushInterval = fileFlushInterval;
        return this;
    }

    public void appendDataByChannel(int offset, ByteBuffer byteBuffer) {
        byteBuffer.flip();
        try {
            fileChannel.write(byteBuffer, offset);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byteBuffer.clear();
    }
}