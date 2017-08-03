package security;

import play.Logger;
import play.cache.Cache;
import play.libs.F;
import play.mvc.Http;
import play.mvc.Result;

/**
 * Created by thomas on 8/3/17.
 */
public class SecureAction extends play.mvc.Action.Simple {

    public F.Promise<Result> call(Http.Context ctx) throws Throwable {
        return delegate.call(ctx);
    }

}
