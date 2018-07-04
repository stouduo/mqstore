package io.openmessaging.model;

import io.openmessaging.config.Config;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class MappedFile {

    // 文件名
    private String fileName;

    // 文件所在目录路径
    private String fileDirPath;

    // 文件对象
    private File file;

    private MappedByteBuffer mappedByteBuffer;
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
    private ScheduledExecutorService ioWorker = Executors.newScheduledThreadPool(1, r -> {
        Thread thread = new Thread(r);
        thread.setName("flush-ioWorker-" + fileName);
        thread.setDaemon(true);
        return thread;
    });

    public MappedFile setFileFlushSize(int size) {
        this.fileFlushSize = size;
        return this;
    }

    public MappedFile(String fileName, String fileDirPath, int fileSize) {
        this.fileName = fileName;
        this.fileDirPath = fileDirPath;
        this.file = new File(fileDirPath);
        if (!file.exists()) file.mkdirs();
        this.file = new File(fileDirPath + File.separator + fileName);
        try {
            file.delete();
            file.createNewFile();
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.fileSize = fileSize;
        boundChannelToByteBuffer();
        ioWorker.scheduleAtFixedRate(this::flush, 0, 1000, TimeUnit.MILLISECONDS);
    }

    private void flush() {
        if (writeSize.get() - lastFlushFileSize >= fileFlushSize) {
            mappedByteBuffer.force();
            lastFlushFileSize = writeSize.get();
            System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:mmm").format(new Date()) + "--" + Thread.currentThread().getName() + ": flush " + fileName + " to disk:" + writeSize.get());
        }
    }

    /**
     * 内存映照文件绑定
     *
     * @return
     */
    public synchronized boolean boundChannelToByteBuffer() {
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            this.fileChannel = raf.getChannel();
        } catch (Exception e) {
            e.printStackTrace();
            this.boundSuccess = false;
            return false;
        }

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
        int position = mappedByteBuffer.position();
        mappedByteBuffer.position(offset);
        ByteBuffer byteBuffer = mappedByteBuffer.slice();
        mappedByteBuffer.position(position);
        byteBuffer.limit(size);
        return byteBuffer;
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

    public int getInt(int offset) {
        return mappedByteBuffer.getInt(offset);
    }

    public long getLong(int offset) {
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
}