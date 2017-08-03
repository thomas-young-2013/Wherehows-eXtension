/**
 * Copyright 2015 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package controllers;

import dao.FlowsDAO;
import dao.UserDAO;
import play.Play;
import play.cache.Cache;
import play.data.DynamicForm;
import play.libs.Json;
import play.mvc.Controller;
import play.mvc.Result;
import play.Logger;
import play.mvc.Security;
import play.mvc.With;
import security.CASUser;
import security.CASUtils;
import security.SecureAction;
import utils.Tree;
import views.html.index;
import views.html.login;
import views.html.lineage;
import views.html.schemaHistory;
import static play.data.Form.form;
import org.apache.commons.lang3.StringUtils;
import security.AuthenticationManager;

public class Application extends Controller
{
    private static String TREE_NAME_SUBFIX = ".tree.name";
    private static String URL_PREFIX = "/wherehows/";
    private static final String APP_URL = Play.application().configuration().getString("application.url");

    @With(SecureAction.class)
    @Security.Authenticated(Secured.class)
    public static Result index()
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        //You cann generate the Csrf token such as String csrfToken = SecurityPlugin.getInstance().getCsrfToken();
        String csrfToken = "";
        return ok(index.render(username, csrfToken));
    }

    @Security.Authenticated(Secured.class)
    public static Result lineage()
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return ok(lineage.render(username, "chains", 0, null, null, null));
    }

    @Security.Authenticated(Secured.class)
    public static Result datasetLineage(int id)
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return ok(lineage.render(username, "dataset", id, null, null, null));
    }

    @Security.Authenticated(Secured.class)
    public static Result metricLineage(int id)
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return ok(lineage.render(username, "metric", id, null, null, null));
    }

    @Security.Authenticated(Secured.class)
    public static Result flowLineage(String application, String project, String flow)
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        String type = "azkaban";
        if (StringUtils.isNotBlank(application) && (application.toLowerCase().indexOf("appworx") != -1))
        {
            type = "appworx";

        }
        return ok(lineage.render(username, type, 0, application.replace(" ", "."), project, flow));
    }

    @Security.Authenticated(Secured.class)
    public static Result schemaHistory()
    {
        String username = session("user");
        if (username == null)
        {
            username = "";
        }
        return ok(schemaHistory.render(username));
    }

    public static Result login()
    {
        String uuid = session("uuid");
        if(uuid == null) {
            uuid = java.util.UUID.randomUUID().toString();
            session("uuid", uuid);
        }

        String originUrl = (String) Cache.get("url_" + session().get("uuid"));
        if (originUrl == null || StringUtils.isEmpty(originUrl)) {
            originUrl = APP_URL;
            Cache.set("url_" + session().get("uuid"), StringUtils.isEmpty(originUrl) ? "/" : originUrl, 60*10);
        }

        Logger.debug("[SecureCAS]: adding url " + originUrl + " into cache with key " + "url_" + session().get("uuid"));

        // we redirect the user to the cas login page
        String casLoginUrl = CASUtils.getCasLoginUrl(false);
        return redirect(casLoginUrl);
    }

    public static Result authenticate()
    {
        Boolean isAuthenticated = Boolean.FALSE;
        String ticket = request().getQueryString("ticket");
        try {
            if (ticket != null) {
                Logger.debug("[SecureCAS]: Try to validate ticket " + ticket);
                CASUser user = CASUtils.valideCasTicket(ticket);
                if (user != null) {
                    isAuthenticated = Boolean.TRUE;
                    String username = user.getUsername();
                    // session.put("username", username);
                    session("user", username);
                    Cache.set("TK_" + ticket, username);
                    // we invoke the implementation of onAuthenticate
                    // Security.invoke("onAuthenticated", user);
                }
            }

            if (isAuthenticated) {
                // we redirect to the original URL
                String url = (String) Cache.get("url_" + session("uuid"));
                Logger.debug("[SecureCAS]: find url " + url + " into cache for the key " + "url_" + session("uuid"));
                // Cache.delete("url_" + session.getId());
                Cache.remove("url_" + session("uuid"));
                if (url == null) {
                    url = "/";
                }
                Logger.debug("[SecureCAS]: redirect to url -> " + url);
                return redirect(url);
            } else {
                return badRequest();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return badRequest();
    }

    public static Result signUp()
    {
        DynamicForm loginForm = form().bindFromRequest();
        String username = loginForm.get("inputName");
        String firstName = loginForm.get("inputFirstName");
        String lastName = loginForm.get("inputLastName");
        String email = loginForm.get("inputEmail");
        String password = loginForm.get("inputPassword");
        String errorMessage = "";
        try
        {
            errorMessage = UserDAO.signUp(username, firstName, lastName, email, password);
            if (StringUtils.isNotBlank(errorMessage))
            {
                flash("error", errorMessage);
            }
            else
            {
                flash("success", "Congratulations! Your account has been created. Please login.");
            }
        }
        catch (Exception e)
        {
            flash("error", e.getMessage());
        }

        // return redirect(controllers.routes.Application.login());
        return redirect(URL_PREFIX+"login");
    }

    public static Result logout()
    {
        session().clear();
        flash("success", "You've been logged out");
        // return redirect(controllers.routes.Application.login());
        return redirect(URL_PREFIX+"login");
    }

    @Security.Authenticated(Secured.class)
    public static Result loadTree(String key)
    {
        if (StringUtils.isNotBlank(key) && key.equalsIgnoreCase("flows")) {
            return ok(FlowsDAO.getFlowApplicationNodes());
        }
        // switch here..
        boolean isFiltered = false;
        if (isFiltered) {
            if (StringUtils.isNotBlank(key)) {
                String username = session("user");
                if (username != null) {
                    String treeName = Play.application().configuration().getString(key + TREE_NAME_SUBFIX);
                    return ok(UserDAO.getUserGroupFileTree(username, treeName));
                }
            }
            return ok(Json.toJson(""));
        } else {
            return ok(Tree.loadTreeJsonNode(key + TREE_NAME_SUBFIX));
        }
    }

    public static Result loadFlowProjects(String app)
    {
        return ok(FlowsDAO.getFlowProjectNodes(app));
    }

    public static Result loadFlowNodes(String app, String project)
    {
        return ok(FlowsDAO.getFlowNodes(app, project));
    }

}
