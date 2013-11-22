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

import static org.junit.Assert.assertTrue;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;


/**
 * {@link MapEvictionTest}
 *
 * @author <a href="mailto:marc.arens@open-xchange.com">Marc Arens</a>
 */
public class MapEvictionTest {

    private static Logger LOG = Logger.getLogger(MapEvictionTest.class);
    private HazelcastInstance hazelcast;
    private final static String MARC = "marc.arens@premium";
    private final static String MARC1 = "ox://"+MARC+"/1";
    private final static String MARC2 = "ox://"+MARC+"/2";
    private final static String STEFFEN = "steffen.templin@premium";
    private final static String STEFFEN1 = "ox://"+STEFFEN+"/1";
    private final static String STEFFEN2 = "ox://"+STEFFEN+"/2";
    private List<String> idsToKeep;
    private List<String> fullIdsToKeep;
    ScheduledExecutorService executorService;

    @Before
    public void setUp() throws Exception {
        idsToKeep = new ArrayList<String>(2);
        fullIdsToKeep = new ArrayList<String>(4);
        executorService = Executors.newScheduledThreadPool(50);

        Config hazelcastConfig = Configs.getHazelcastConfig();
        hazelcastConfig.addMultiMapConfig(Configs.getIDMapConfig());
        hazelcastConfig.addMapConfig(Configs.getResourceMapConfig());
        LOG.info("Starting hazelcast with config:\n" + hazelcastConfig);
        hazelcast = Hazelcast.newHazelcastInstance(hazelcastConfig);

        addEvictionListener();
        fillMaps();
        startRefreshTimer();
    }


    private void startFluctuation() throws Exception {
        LOG.info("Start fluctuation...");
        Collection<Runnable> workers = new ArrayList<Runnable>();
        for(int i = 0; i < 10; i++) {
            workers.add(new Runnable() {

                @Override
                public void  run() {
                    boolean running = true;
                    while (running) {
                        String generalClient = IDGenerator.getNewGeneralID();
                        String fullClient = IDGenerator.getNewFullID(generalClient);
                        LOG.debug("Thread " + Thread.currentThread().getId() + " is adding " + generalClient);
                        getIDMapping().put(generalClient, fullClient);
                        getResourceMapping().put(fullClient, createRandomMap());
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            LOG.info("Interrupted, cleaning up before termination.");
                            running=false;
                        }
                        LOG.debug("Thread " + Thread.currentThread().getId() + " is removing " + generalClient);
                        getResourceMapping().remove(fullClient);
//                        getIDMapping().remove(generalClient, fullClient);
                    }
                }
                
            });
        }
        for (Runnable runnable : workers) {
            executorService.submit(runnable);
        }
    }

    /**
     * Get the mapping of general IDs to full IDs e.g. marc.arens@premium <-> ox://marc.arens@premium/random.
     * 
     * @return the map used for mapping general IDs to full IDs.
     */
    private MultiMap<String, String> getIDMapping() {
        return hazelcast.getMultiMap(Configs.ID_MAP);
    }

    /**
     * Get the mapping of full IDs to the Resource e.g. ox://marc.arens@premuim/random <-> ResourceMap.
     * 
     * @return the map used for mapping full IDs to ResourceMaps.
     */
    private IMap<String, Map<String, Serializable>> getResourceMapping() {
        return hazelcast.getMap(Configs.RESOURCE_MAP);
    }

    /**
     * Fill the maps with the basic entries that shouldn't get evicted.
     */
    private void fillMaps() throws Exception {
        MultiMap<String, String> idMapping = getIDMapping();
        idMapping.put(STEFFEN, STEFFEN1);
        idMapping.put(STEFFEN, STEFFEN2);
        idMapping.put(MARC, MARC1);
        idMapping.put(MARC, MARC2);
        
        IMap<String,Map<String,Serializable>> resourceMapping = getResourceMapping();
        resourceMapping.put(MARC1, createRandomMap());
        resourceMapping.put(MARC2, createRandomMap());
        resourceMapping.put(STEFFEN1, createRandomMap());
        resourceMapping.put(STEFFEN2, createRandomMap());

        idsToKeep.add(STEFFEN);
        idsToKeep.add(MARC);
        fullIdsToKeep.add(STEFFEN1);
        fullIdsToKeep.add(STEFFEN2);
        fullIdsToKeep.add(MARC1);
        fullIdsToKeep.add(MARC2);
    }

    private Map<String, Serializable> createRandomMap() {
        HashMap<String, Serializable> map = new HashMap<String, Serializable>();
        map.put("uuid", UUID.randomUUID());
        map.put("timestamp", new Date());
        return map;
    }

    private void addEvictionListener() throws Exception {
        getResourceMapping().addEntryListener(new EntryListener<String, Map<String, Serializable>>() {

            @Override
            public void entryUpdated(EntryEvent<String, Map<String, Serializable>> event) {}

            @Override
            public void entryRemoved(EntryEvent<String, Map<String, Serializable>> event) {
                String fullID = event.getKey();
                String generalID = fullID.substring(5, fullID.lastIndexOf("/"));
                boolean removed = getIDMapping().remove(generalID, fullID);
                if(removed) {
                    LOG.debug("Removed " + fullID + " from idMapping");
                } else {
                    LOG.error("Failed to remove "+ fullID + " from idMapping");
                }
            }

            @Override
            public void entryAdded(EntryEvent<String, Map<String, Serializable>> event) {}

            @Override
            public void entryEvicted(EntryEvent<String, Map<String, Serializable>> event) {
                String id = event.getKey();
                Object source = event.getSource();
                Member member = event.getMember();
                if(fullIdsToKeep.contains(id)) {
                    LOG.error("Source " + source + " on Member: " + member + " fired event. " + "Evicting mapping for " + id + " with owner " + getOwner(id));
                }
            }
        }, false);
    }

    private Member getOwner(String id) {
        PartitionService partitionService = Hazelcast.getPartitionService();
        Partition partition = partitionService.getPartition(id);
        return partition.getOwner();
    }

    /**
     * Starts the timer that refreshes resources we want to keep/prevent eviction
     */
    private void startRefreshTimer() {
        executorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                MultiMap<String,String> idMapping = getIDMapping();
                IMap<String,Map<String,Serializable>> resourceMapping = getResourceMapping();
                LOG.info("--------------------");
                for (String generalID : idsToKeep) {
                    Collection<String> fullIDs = idMapping.get(generalID);
                    for (String fullID : fullIDs) {
                        /**
                         * Maximum number of seconds for each entry to stay idle in the map. Entries that are idle(not touched) for more
                         * than <max-idle-seconds> will get automatically evicted from the map. Entry is touched if get, put or containsKey
                         * is called.
                         * --> Nothing should get evicted from the resourceMapping
                         */
                        Map<String, Serializable> map = resourceMapping.get(fullID);
                        LOG.info("Value for " + fullID + " with owner " + getOwner(fullID) + " was " + map);
                    }
                }
                LOG.info("--------------------");
            }

        }, 1, 10, TimeUnit.SECONDS);
    }

    @Test
    public void test() throws Exception {
        startFluctuation();
        LOG.info("IDs: " + getIDMapping().entrySet());
        LOG.info("Resources: "+ getResourceMapping().entrySet());
        Thread.sleep(240000);
        assertTrue("Entry shouldn't have been evicted", getResourceMapping().containsKey(MARC1));
        assertTrue("Entry shouldn't have been evicted", getResourceMapping().containsKey(MARC2));
        assertTrue("Entry shouldn't have been evicted", getResourceMapping().containsKey(STEFFEN1));
        assertTrue("Entry shouldn't have been evicted", getResourceMapping().containsKey(STEFFEN2));
        executorService.shutdownNow();
    }

}
