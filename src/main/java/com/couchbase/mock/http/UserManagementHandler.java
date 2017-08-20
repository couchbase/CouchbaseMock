/*
 * Copyright 2017 Couchbase, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.couchbase.mock.http;

import com.couchbase.mock.CouchbaseMock;
import com.couchbase.mock.JsonUtils;
import com.couchbase.mock.httpio.HandlerUtil;
import com.couchbase.mock.httpio.HttpServer;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.protocol.HttpContext;
import org.apache.http.protocol.HttpRequestHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Mike Goldsmith
 */
public class UserManagementHandler {

    private final CouchbaseMock mock;

    public UserManagementHandler(CouchbaseMock mock) {
        this.mock = mock;
    }

    public void register(HttpServer server) {
        server.register("/settings/rbac/users/local*", getUsersHandler);
    }

    private final HttpRequestHandler getUsersHandler = new HttpRequestHandler() {
        @java.lang.Override
        public void handle(HttpRequest httpRequest, HttpResponse httpResponse, HttpContext httpContext) throws HttpException, IOException {

            String method = httpRequest.getRequestLine().getMethod();
            String username = getUsername(httpRequest);
            String password = null;
            Boolean userExists = mock.getUsers().containsKey(username);

            if (method.equals("GET")) {
                // if no username, return all users
                if (username.isEmpty()) {
                    ArrayList<User> localUsers = new ArrayList<User>();
                    localUsers.addAll(mock.getUsers().values());
                    HandlerUtil.makeJsonResponse(httpResponse, JsonUtils.encode(localUsers));
                    return;
                }

                // if user doesn't exist, return bad request
                if (!userExists) {
                    httpResponse.setStatusCode(HttpStatus.SC_NOT_FOUND);
                    return;
                }

                // return user as JSON
                User user = mock.getUsers().get(username);
                HandlerUtil.makeJsonResponse(httpResponse, JsonUtils.encode(user));
                return;

            } else if (method.equals("PUT") && !username.isEmpty() && httpRequest instanceof HttpEntityEnclosingRequest) {
                User user;
                ArrayList<Role> roles = new ArrayList<Role>();
                if (userExists) {
                    user = mock.getUsers().get(username);
                } else {
                    user = new User("local", username);
                }

                // convert query string to key value pairs
                HttpEntity entity = ((HttpEntityEnclosingRequest) httpRequest).getEntity();
                List<NameValuePair> content = URLEncodedUtils.parse(entity);

                for (NameValuePair pair : content) {
                    if ("roles".equals(pair.getName())) {
                        Pattern pattern = Pattern.compile("(.+)(\\[(.+)])");
                        String rolesEncoded = pair.getValue();
                        String[] rolesList = rolesEncoded.split(",");
                        for (String role : rolesList) {
                            Matcher matcher = pattern.matcher(role);
                            if (matcher.find()) {
                                roles.add(new Role(matcher.group(1), matcher.group(3)));
                            }
                        }
                    } else if("password".equals(pair.getName())) {
                        password = pair.getValue();
                    } else if ("name".equals(pair.getName())) {
                        user.setName(pair.getValue());
                    }
                }

                // if no roles provided, return bad request
                if (roles.size() == 0) {
                    httpResponse.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                    return;
                }

                // replace role set
                user.setRoles(roles);

                // if new user and no password provided, return bad request
                if (!userExists && (password == null || password.isEmpty())) {
                    httpResponse.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                    return;
                }

                // add user to user list
                mock.getUsers().put(username, user);
                return;
            } else if (method.equals("DELETE") && !username.isEmpty()) {
                // if user exists, remove from list
                if (userExists) {
                    mock.getUsers().remove(username);
                    return;
                }
            }

            // no good path found, return bad request
            httpResponse.setStatusCode(HttpStatus.SC_BAD_REQUEST);
        }
    };

    private String getUsername(HttpRequest httpRequest) {
        String username = "";
        String[] parts = httpRequest.getRequestLine().getUri().split("local/");
        if (parts.length == 2) {
            username = parts[1];
        }

        return username;
    }
}
