/*
 * Copyright 2016, Simon Matić Langford
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.co.exemel.opentsdb;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;
import com.timgroup.statsd.NonBlockingStatsDClient;
import com.timgroup.statsd.StatsDClient;
import net.opentsdb.stats.LatencyStatsPlugin;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.Config;

/**
 *
 */
public class StatsdLatencyPlugin extends LatencyStatsPlugin {
    
    private static final String CONFIG_STATSD_HOST = "tsd.latency_stats.statsd.host";
    private static final String CONFIG_STATSD_PORT = "tsd.latency_stats.statsd.port";
    
    private String packetString;
    
    private String statsdHost;
    private int statsdPort;
    private StatsDClient statsd;

    /**
     * Called by TSDB to initialize the plugin
     * Implementations are responsible for setting up any IO they need as well
     * as starting any required background threads.
     * <b>Note:</b> Implementations should throw exceptions if they can't start
     * up properly. The TSD will then shutdown so the operator can fix the
     * problem. Please use IllegalArgumentException for configuration issues.
     *
     * @param config The TSDB configuration
     * @param metricName The name of the metric to emit aggregations to
     * @param xtratag    Extra tags to use when emitting aggregations
     * @throws IllegalArgumentException if required configuration parameters are
     *                                  missing
     */
    @Override
    public void initialize(final Config config, String metricName, String xtratag) {
        this.packetString = metricName;
        if (!Strings.isNullOrEmpty(xtratag)) {
            for (String kv : xtratag.split(" ")) {
                String[] pair = kv.split("=");
                packetString += "._t_" + pair[0].replace(".","_") + "." + pair[1].replace(".","_");
            }
        }
        this.statsdHost = config.getString(CONFIG_STATSD_HOST);
        this.statsdPort = config.hasProperty(CONFIG_STATSD_PORT) ? config.getInt(CONFIG_STATSD_PORT) : 8125;
        
        
    }

    /**
     * Called when this plugin is live. Calls to {@link #add(int)} will not be made
     * before this method is called. Under race conditions it's possible this method will never
     * be called on a given instance of this class.
     */
    @Override
    public void start() {
        statsd = new NonBlockingStatsDClient("", statsdHost, statsdPort);
    }

    /**
     * Called when the TSD is shutting down to gracefully flush any buffers or
     * close open connections.
     */
    @Override
    public Deferred<Object> shutdown() {        
        statsd.stop();
        return Deferred.fromResult(null);
    }

    /**
     * Should return the version of this plugin in the format:
     * MAJOR.MINOR.MAINT, e.g. 2.0.1. The MAJOR version should match the major
     * version of OpenTSDB the plugin is meant to work with.
     *
     * @return A version string used to log the loaded version
     */
    @Override
    public String version() {
        return "2.0.0";
    }

    /**
     * Called by the TSD when a request for statistics collection has come in. The
     * implementation may provide one or more statistics. If no statistics are
     * available for the implementation, simply stub the method. This method is responsible
     * both for collecting stats about the plugin, as well as emitting aggregations of the
     * measurements it has been collecting
     *
     * @param collector  The collector used for emitting statistics
     */
    @Override
    public void collectStats(final StatsCollector collector) {
        // no-op
    }

    /**
     * Adds a value to be measured.
     *
     * @param value The value to add.
     * @throws IllegalArgumentException may be thrown if the value given is negative.
     */
    @Override
    public void add(int value) {
        statsd.recordExecutionTime(packetString, value);
    }
}
