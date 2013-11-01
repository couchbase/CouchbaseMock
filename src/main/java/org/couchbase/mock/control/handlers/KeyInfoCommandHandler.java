/*
 * Copyright 2013 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.couchbase.mock.control.handlers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.control.CommandStatus;
import org.couchbase.mock.memcached.Item;
import org.couchbase.mock.memcached.KeySpec;
import org.couchbase.mock.memcached.MemcachedServer;
import org.couchbase.mock.memcached.VBucketInfo;
import org.jetbrains.annotations.NotNull;

/**
 *
 * @author Mark Nunberg <mnunberg@haskalah.org>
 */
public final class KeyInfoCommandHandler extends KeyCommandHandler {
    private String result;

    private static Map<String,Object> itemToString(Item itm) {
        Map<String,Object> ret = new HashMap<String, Object>();
        if (itm != null) {
            byte[] value = itm.getValue();
            String valueString = null;
            if (value != null) {
                valueString = new String(value);
            }
            ret.put("Value", valueString);
            ret.put("CAS", itm.getCas());
        }
        return ret;
    }

    private static Map<String,Object> configToString(MemcachedServer server, KeySpec ks)
    {
        VBucketInfo vbi = server.getStorage().getVBucketInfo(ks.vbId);
        Map<String,Object> ret = new HashMap<String, Object>();
        if (vbi.getOwner() == server) {
            ret.put("Index", 0);
            ret.put("Type", "master");
        } else {
            ret.put("Index", vbi.getReplicas().indexOf(server) + 1);
            ret.put("Type", "replica");
        }
        return ret;
    }

    private static Map<String,Object> serverKeyStatus(MemcachedServer server, KeySpec ks)
    {
        Map<String,Object> ret = new HashMap<String, Object>();
        ret.put("Cache", itemToString(server.getStorage().getCached(ks)));
        ret.put("Disk", itemToString(server.getStorage().getPersisted(ks)));
        ret.put("Conf", configToString(server, ks));

        return ret;
    }

    @NotNull
    @Override
    public CommandStatus execute(@NotNull CouchbaseMock mock, @NotNull Command command, @NotNull JsonObject payload) {
        super.execute(mock, command, payload);
        List<Object> infoList = new ArrayList<Object>();
        List<MemcachedServer> vbiServers = vbi.getAllServers();

        for (MemcachedServer server : bucket.getServers()) {
            if (!vbiServers.contains(server)) {
                infoList.add(null);
                continue;
            }

            Map<String,Object> skInfo = serverKeyStatus(server, keySpec);
            infoList.add(skInfo);
        }

        CommandStatus ret = new CommandStatus();
        ret.setPayload(infoList);
        return ret;
    }
}
