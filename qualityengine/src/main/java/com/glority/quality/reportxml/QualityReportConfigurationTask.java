package com.glority.quality.reportxml;

import org.apache.tools.ant.BuildException;

import com.glority.quality.BaseTask;
import com.glority.quality.StringUtil;
import com.glority.quality.connectors.vdi.ConfigurationCSV;

/**
 * The task to initial configuration module in quality report
 * 
 * It accepts 2 input mode: 1. Configurations.CSV generated by glority VDI 2. os, lang, arch, and softwares. This input
 * is higher priority
 * 
 * The format of softwares string should like this: <software1>,<softwarevalue>;<software2>,<softwarevalue> the
 * softwares is splited by ";" while each software item have name and value that splited by ","
 * 
 * 
 * @author XQS
 * 
 */
public class QualityReportConfigurationTask extends BaseTask {
    private String configurationCSVPath;
    private String os;
    private String lang;
    private String arch;
    private String softwares;
    private String id;

    @Override
    public void process() {
        ConfigurationCSV csv = new ConfigurationCSV();
        csv.setCsvPath(configurationCSVPath);
        try {
            csv.parseCSV();
        } catch (Exception e) {
            getProject().log("Warning: failed to load csv file: " + configurationCSVPath);
        }
        if (null == id) {
            throw new BuildException("Configuration id is null");
        }
        csv.getCfg().setId(id);
        if (!StringUtil.isEmpty(os)) {
            csv.getCfg().getEnvironments().setOs(os);
        }
        if (!StringUtil.isEmpty(lang)) {
            csv.getCfg().getEnvironments().setLanguage(lang);
        }
        if (!StringUtil.isEmpty(arch)) {
            csv.getCfg().getEnvironments().setArch(arch);
        }
        if (!StringUtil.isEmpty(softwares)) {
            String[] ss = softwares.split(";");
            for (String s : ss) {
                csv.getCfg().addSoftware(s);
            }
        }

        qualityReport.addConfiguration(csv.getCfg());
        exportQualityXml();
    }

    public void setConfigurationCSVPath(String configurationCSVPath) {
        this.configurationCSVPath = configurationCSVPath;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public void setArch(String arch) {
        this.arch = arch;
    }

    public void setSoftwares(String softwares) {
        this.softwares = softwares;
    }
}
