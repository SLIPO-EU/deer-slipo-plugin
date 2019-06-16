package org.aksw.deer.plugin.slipo;

import org.pf4j.Plugin;
import org.pf4j.PluginWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlipoPlugin extends Plugin {

    private static final Logger logger = LoggerFactory.getLogger(SlipoPlugin.class);

    public SlipoPlugin(PluginWrapper wrapper) {
        super(wrapper);
    }

    @Override
    public void start() {
        logger.info("SlipoPlugin.start()");
    }

    @Override
    public void stop() {
        logger.info("SlipoPlugin.stop()");
    }

}