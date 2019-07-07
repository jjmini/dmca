package com.glority.qualityserver.service.impl;

import org.apache.log4j.Logger;
import org.springframework.stereotype.Service;

import com.glority.qualityserver.ServerErrorCollector;
import com.glority.qualityserver.model.Server;
import com.glority.qualityserver.service.qualitysystemErrorService;

/**
 * qualitysystemErrorServiceImpl.
 * 
 * @author liheping
 * 
 */
@Service
public class qualitysystemErrorServiceImpl implements qualitysystemErrorService {
    private static final Logger LOGGER = Logger.getLogger(qualitysystemErrorServiceImpl.class);


    @Override
    public void doProcessAlerts(Server server, String detail) {
        LOGGER.error("server - " + server.getId() + " " + detail);
        ServerErrorCollector.getInstance().collectError(detail, "server : " + server.getName());
    }

}
