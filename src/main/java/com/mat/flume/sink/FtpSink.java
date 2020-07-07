package com.mat.flume.sink;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.flume.*;
import org.apache.flume.conf.BatchSizeSupported;
import org.apache.flume.conf.Configurable;
import org.apache.flume.instrumentation.SinkCounter;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FtpSink extends AbstractSink  implements Configurable, BatchSizeSupported {

    private static final Logger logger = LoggerFactory.getLogger(FtpSink.class);

    private String host;
    private int port;
    private String user;
    private String password;
    private String ftpDirPath;
    private String suffix;
    private String tempSuffix;
    private String prefix;

    private FTPClient ftpClient;

    private static final int defaultBatchSize = 1000;
    private int batchSize;

    private static final String WORK_DIR_PATH = "/tmp/flume/";
    private File workDir = new File(WORK_DIR_PATH);

    private SinkCounter sinkCounter;


    @Override
    public void configure(Context context) {
        host = context.getString("host");
        port = context.getInteger("port", 21);
        user = context.getString("user");
        password = context.getString("password");
        ftpDirPath = context.getString("ftpDirPath", "/");
        suffix = context.getString("suffix", ".dat");
        tempSuffix = context.getString("tempSuffix", ".tmp");
        prefix = context.getString("prefix", "DATA-");

        batchSize = context.getInteger("batchSize", defaultBatchSize);

        ftpClient = new FTPClient();

        if(!workDir.exists()) {
            workDir.mkdir();
        }
        if (sinkCounter == null) {
            sinkCounter = new SinkCounter(getName());
        }

        logger.info("Loading FtpSink config: host={}, port={}, user={}, password={}", host, port, user, password);
    }

    @Override
    public synchronized void start() {
        sinkCounter.start();
        connectServer();
        super.start();
    }

    @Override
    public synchronized void stop() {
        disConnectServer();
        sinkCounter.stop();
        super.stop();
    }

    @Override
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        try {
            transaction.begin();
            verifyConnection();

            List<Event> batch = new ArrayList<>();

            for (int i = 0; i < batchSize; i++) {
                Event event = channel.take();
                if (event == null) {
                    break;
                }

                batch.add(event);
            }

            if (batch.size() == 0) {
                sinkCounter.incrementBatchEmptyCount();
                status = Status.BACKOFF;
            } else {
                if (batch.size() < batchSize) {
                    sinkCounter.incrementBatchUnderflowCount();
                } else {
                    sinkCounter.incrementBatchCompleteCount();
                }
                sinkCounter.addToEventDrainAttemptCount(batch.size());
                dealEventList(batch);
            }


            transaction.commit();
            sinkCounter.addToEventDrainSuccessCount(batch.size());
        } catch (Exception ex) {
            logger.error(ex.getMessage(), ex);
            sinkCounter.incrementEventWriteOrChannelFail(ex);
            transaction.rollback();
            throw new EventDeliveryException("Failed to send events", ex);
        } finally {
            transaction.close();
        }

        return status;
    }


    private void connectServer() {
        for (int i = 5; ; i+=5) {
            try {
                ftpClient.connect(host, port);
                ftpClient.login(user, password);

                int reply = ftpClient.getReplyCode();
                if (!FTPReply.isPositiveCompletion(reply)) {
                    logger.error("ftp reply failed, reply-code:" + reply);
                    ftpClient.disconnect();
                    throw new IOException("ftp reply failed, reply-code:" + reply);
                }

                ftpClient.enterLocalPassiveMode();
                ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
                boolean dirFlag = ftpClient.changeWorkingDirectory(ftpDirPath);
                if (!dirFlag) {
                    logger.error("change ftp dir failed, dir:" + ftpDirPath);
                    throw new IOException("change ftp dir failed, dir:" + ftpDirPath);
                }
                logger.info("ftp server is connected");
                sinkCounter.incrementConnectionCreatedCount();
                break;
            } catch (IOException e) {
                sinkCounter.incrementConnectionFailedCount();
                logger.error("Can't connect FtpServer, {}:{}", host, port);
                logger.error(e.getMessage(), e);
                logger.info("{}s will retry to connect", i);
                try {
                    Thread.sleep(i * 1000L);
                } catch (InterruptedException ex) {
                    logger.error(ex.getMessage(), ex);
                }
            }
        }
    }

    private void disConnectServer() {
        try {
            ftpClient.logout();
            ftpClient.disconnect();
            sinkCounter.incrementConnectionClosedCount();
        } catch (IOException e) {
            sinkCounter.incrementConnectionFailedCount();
            logger.error(e.getMessage(), e);
        }

    }

    @Override
    public long getBatchSize() {
        return batchSize;
    }

    private void verifyConnection() {
        if (!ftpClient.isConnected()) {
             connectServer();
        }
    }

    private void renameFtpFile(String srcFile, String targetFile) throws Exception {
        try {
            boolean renameFlag = ftpClient.rename(srcFile, targetFile);
            if (!renameFlag) {
                int code = ftpClient.getReplyCode();
                logger.error("ftp-file rename failed, ftp-code=" + code);
                throw new Exception("ftp-file rename failed, ftp-code=" + code);
            }
            logger.info("ftp rename success, file={}", targetFile);
        } catch (IOException e) {
            logger.error("ftp-file rename exception:" + e.getMessage());
            logger.error(e.getMessage(), e);
            throw e;
        }
    }


    private void dealEventList(List<Event> eventList) throws Exception {
        if (eventList.size() == 0) {
            return;
        }

        logger.info("deal data numbers: {}", eventList.size());

        String filename = prefix + System.currentTimeMillis() + suffix + tempSuffix;
        File localFile = new File(WORK_DIR_PATH + filename);
        if (!localFile.exists()) {
            boolean newFileFlag = localFile.createNewFile();
            if (!newFileFlag) {
                throw new Exception("local temp file create failed");
            }
        }

        FileWriter fw = new FileWriter(localFile);
        for (int i = 0; i < eventList.size(); i ++) {
            if (i == eventList.size() - 1) {
                fw.write(new String(eventList.get(i).getBody()));
            } else {
                fw.write(new String(eventList.get(i).getBody()) + "\n");
            }
        }

        fw.flush();
        fw.close();

        // upload ftp
        FileInputStream fis = new FileInputStream(localFile);
        boolean storeFileFlag = ftpClient.storeFile(filename, fis);
        fis.close();
        if (!storeFileFlag) {
            int code = ftpClient.getReplyCode();
            logger.error("file upload to ftp failed, ftp-code={},  filename={}", code, filename);
            throw new Exception("file upload to ftp failed, ftp-code=" + code + ", filename=" + filename);
        }

        logger.info("ftp upload success!, filename={}", filename);
        fis.close();

        // rename ftp file
        String ftpFilename = filename.substring(0, filename.indexOf(tempSuffix));
        renameFtpFile(filename, ftpFilename);

        boolean deleteFlag = localFile.delete();
        if (!deleteFlag) {
            logger.warn("local file deleted failed, file={}", filename);
        }
    }
}
