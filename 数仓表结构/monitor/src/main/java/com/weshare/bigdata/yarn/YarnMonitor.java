package com.weshare.bigdata.yarn;

import com.weshare.bigdata.util.SendEmailUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author yuheng.wang
 * @date 2020/10/20 10:14
 * @Description
 */
public class YarnMonitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(YarnMonitor.class);

    public static void main(String[] args) throws InterruptedException {
        //calculate jobName : Flink per-job cluster
        String arg = null;
        if (args.length > 0) {
            arg = args[0];
            LOGGER.warn("job name : {}",arg);
        } else {
            LOGGER.error("you must enter a job name!");
            return;
        }
        while (true) {
            boolean isRunning = yarnIsRunning(arg);
            if (!isRunning) {
                SendEmailUtil.sendEmail("Yarn \"Flink per-job cluster\" Application Monitor",
                        "Flink calculate job has been killed!");
                LOGGER.warn("Flink job is not running!");
            } else {
                LOGGER.warn("Flink job is running,the job name : {}",arg);
            }
            TimeUnit.MINUTES.sleep(30);
//            TimeUnit.SECONDS.sleep(5);
        }
    }

    /**
     * initialize Yarn client connction
     * @return
     */
    public static YarnClient initYarnClient() {
        YarnClient yarnClient = YarnClient.createYarnClient();
        YarnConfiguration conf = new YarnConfiguration();
        conf.set("yarn.resourcemanager.hostname","node148");
        yarnClient.init(conf);
        yarnClient.start();
        return yarnClient;
    }

    public static void closeYarnClient(YarnClient yarnClient) {
        try {
            yarnClient.close();
        } catch (IOException e) {
            LOGGER.error("failed to close Yarn client connection.",e);
        }
    }

    /**
     * get application id by the job name on Yarn
     * @param jobName
     * @return : applicationId
     */
    public static String getAppId(String jobName) {
        YarnClient yarnClient = initYarnClient();
        //initialize Yarn application state
        EnumSet<YarnApplicationState> appStates = EnumSet.noneOf(YarnApplicationState.class);
        if (appStates.isEmpty()) {
            appStates.add(YarnApplicationState.RUNNING);
            appStates.add(YarnApplicationState.ACCEPTED);
            appStates.add(YarnApplicationState.SUBMITTED);
        }
        //create Yarn application List
        List<ApplicationReport> appReports = null;
        //get the running application
        try {
            appReports = yarnClient.getApplications(appStates);
        } catch (YarnException e) {
            LOGGER.error("failed to get Yarn applications.", e);
        } catch (IOException e) {
            LOGGER.error("failed to get Yarn applications.", e);
        }
        if (appReports != null) {
            for (ApplicationReport appReport : appReports) {
                //get the job name
                String appName = appReport.getName();
                String appType = appReport.getApplicationType();
                if (appName.equals(jobName) && "Apache Flink".equals(appType)) {
                    closeYarnClient(yarnClient);
                    return appReport.getApplicationId().toString();
                }
            }
        }
        closeYarnClient(yarnClient);
        return null;
    }

    /**
     *
     * @param applicationId
     * @return application state
     */
    public static YarnApplicationState getAppState(String applicationId) {
        YarnClient yarnClient = initYarnClient();
        ApplicationId appId = ApplicationId.fromString(applicationId);
        YarnApplicationState yarnApplicationState = null;
        try {
            ApplicationReport appReport = yarnClient.getApplicationReport(appId);
            yarnApplicationState = appReport.getYarnApplicationState();
        } catch (YarnException e) {
            LOGGER.error("failed to get Yarn application by applicationId.",e);
        } catch (IOException e) {
            LOGGER.error("failed to get Yarn application by applicationId.",e);
        }
        closeYarnClient(yarnClient);
        return yarnApplicationState;
    }

    /**
     * determine whether the job is running
     * @param appName
     * @return
     */
    public static boolean yarnIsRunning(String appName) {
        YarnClient yarnClient = initYarnClient();
        boolean isRunning = true;
        String appId = getAppId(appName);
        if (StringUtils.isBlank(appId)) {
            isRunning = false;
        } else {
            LOGGER.warn("the applicationId : " + appId);
        }
        return isRunning;
    }
}
