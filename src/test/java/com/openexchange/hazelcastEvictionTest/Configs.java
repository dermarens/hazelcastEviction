/*
 *
 *    OPEN-XCHANGE legal information
 *
 *    All intellectual property rights in the Software are protected by
 *    international copyright laws.
 *
 *
 *    In some countries OX, OX Open-Xchange, open xchange and OXtender
 *    as well as the corresponding Logos OX Open-Xchange and OX are registered
 *    trademarks of the Open-Xchange, Inc. group of companies.
 *    The use of the Logos is not covered by the GNU General Public License.
 *    Instead, you are allowed to use these Logos according to the terms and
 *    conditions of the Creative Commons License, Version 2.5, Attribution,
 *    Non-commercial, ShareAlike, and the interpretation of the term
 *    Non-commercial applicable to the aforementioned license is published
 *    on the web site http://www.open-xchange.com/EN/legal/index.html.
 *
 *    Please make sure that third-party modules and libraries are used
 *    according to their respective licenses.
 *
 *    Any modifications to this package must retain all copyright notices
 *    of the original copyright holder(s) for the original code used.
 *
 *    After any such modifications, the original and derivative code shall remain
 *    under the copyright of the copyright holder(s) and/or original author(s)per
 *    the Attribution and Assignment Agreement that can be located at
 *    http://www.open-xchange.com/EN/developer/. The contributing author shall be
 *    given Attribution for the derivative code and a license granting use.
 *
 *     Copyright (C) 2004-2012 Open-Xchange, Inc.
 *     Mail: info@open-xchange.com
 *
 *
 *     This program is free software; you can redistribute it and/or modify it
 *     under the terms of the GNU General Public License, Version 2 as published
 *     by the Free Software Foundation.
 *
 *     This program is distributed in the hope that it will be useful, but
 *     WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *     or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 *     for more details.
 *
 *     You should have received a copy of the GNU General Public License along
 *     with this program; if not, write to the Free Software Foundation, Inc., 59
 *     Temple Place, Suite 330, Boston, MA 02111-1307 USA
 *
 */

package com.openexchange.hazelcastEvictionTest;

import java.net.InetAddress;
import java.net.UnknownHostException;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NetworkConfig;

/**
 * {@link Configs}
 * 
 * @author <a href="mailto:marc.arens@open-xchange.com">Marc Arens</a>
 */
public class Configs {

    public final static String ID_MAP = "ID_MAP";
    public final static String RESOURCE_MAP = "RESOURCE_MAP";
    
    public static Config getHazelcastConfig() throws UnknownHostException {
        Config config = new Config();
        config.setProperty("hazelcast.logging.type", "log4j");
        config.getGroupConfig().setName("ox");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAwsConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).setConnectionTimeoutSeconds(10);
        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("192.168.32.50");
        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("192.168.33.153");

        NetworkConfig networkConfig = config.getNetworkConfig();
        String hostAddress = InetAddress.getLocalHost().getHostAddress();
        networkConfig.getInterfaces().setEnabled(true).addInterface(hostAddress);
        return config;
    }

    public static MultiMapConfig getIDMapConfig() {
        MultiMapConfig config = new MultiMapConfig();
        config.setName(ID_MAP);
        return config;
    }

    public static MapConfig getResourceMapConfig() {
        MapConfig config = new MapConfig();
        config.setName(RESOURCE_MAP);
        config.setBackupCount(1);
        config.setReadBackupData(false);
        config.setMaxIdleSeconds(20);
        return config;
    }
}
