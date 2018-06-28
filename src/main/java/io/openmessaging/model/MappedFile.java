package io.openmessaging.model;

import io.openmessaging.config.Config;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

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

    // 文件最大只能为50MB
    private final static long MAX_FILE_SIZE = 1024 * 1024 * 50;

    // 最大的脏数据量512KB,系统必须触发一次强制刷
    private long MAX_FLUSH_DATA_SIZE = Config.fileFlushSize;

    // 最大的刷间隔,系统必须触发一次强制刷
    private long MAX_FLUSH_TIME_GAP = Config.fileFlushInterval;

    // 最后一次刷数据的时候
    private long lastFlushTime;

    // 上一次刷的文件位置
    private long lastFlushFilePosition = 0;

    public MappedFile(String fileName, String fileDirPath, int fileSize) {
        super();
        this.fileName = fileName;
        this.fileDirPath = fileDirPath;
        this.file = new File(fileDirPath);
        if (!file.exists()) file.mkdirs();
        this.file = new File(fileDirPath + File.separator + fileName);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        this.fileSize = fileSize;

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
    public boolean appendData(byte[] data) throws Exception {
        return appendData(data, 0, data.length);
    }

    public boolean appendData(byte[] data, int offset, int length) throws Exception {
        int writePosition = mappedByteBuffer.position() + length;
        if (writePosition > fileSize) {   // 如果写入data会超出文件大小限制，不写入
            flush(writePosition);
            writePosition = writePosition - length;
            System.out.println("File="
                    + file.toURI().toString()
                    + " is written full.");
            System.out.println("already write data length:"
                    + writePosition
                    + ", max file size=" + fileSize);
            return false;
        }
        this.mappedByteBuffer.put(data, offset, length);
        // 检查是否需要把内存缓冲刷到磁盘
        if ((writePosition - lastFlushFilePosition > this.MAX_FLUSH_DATA_SIZE)
                ||
                (System.currentTimeMillis() - lastFlushTime > this.MAX_FLUSH_TIME_GAP
                        && writePosition > lastFlushFilePosition)) {
            flush(writePosition);   // 刷到磁盘
        }

        return true;
    }

    public void flush(int writePosition) {
        this.mappedByteBuffer.force();
        this.lastFlushTime = System.currentTimeMillis();
        this.lastFlushFilePosition = writePosition;
    }

    public long getLastFlushTime() {
        return lastFlushTime;
    }

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

    public static long getMaxFileSize() {
        return MAX_FILE_SIZE;
    }


    public long getLastFlushFilePosition() {
        return lastFlushFilePosition;
    }

    public long getMAX_FLUSH_DATA_SIZE() {
        return MAX_FLUSH_DATA_SIZE;
    }

    public long getMAX_FLUSH_TIME_GAP() {
        return MAX_FLUSH_TIME_GAP;
    }

}