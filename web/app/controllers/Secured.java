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

import org.apache.commons.lang3.StringUtils;
import play.Logger;
import play.Play;
import play.cache.Cache;
import play.mvc.Security;
import play.mvc.Http.Context;
import play.mvc.Result;

import static play.mvc.Controller.session;

public class Secured extends Security.Authenticator
{
  @Override
  public String getUsername(Context ctx) {
    return ctx.session().get("user");
  }

  @Override
  public Result onUnauthorized(Context ctx)
  {
    if (!ctx.session().containsKey("uuid")) ctx.session().put("uuid", java.util.UUID.randomUUID().toString());
    String uuid = ctx.session().get("uuid");
    if(uuid == null) {
      uuid = java.util.UUID.randomUUID().toString();
      ctx.session().put("uuid", uuid);
    }

    String originUrl = ctx.request().path();
    Cache.set("url_" + session().get("uuid"), StringUtils.isEmpty(originUrl) ? "/" : originUrl);

    return redirect("/wherehows/login");
  }
}
