package org.couchbase.mock.control.handlers;

import com.google.gson.JsonObject;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.control.MissingRequiredFieldException;
import org.couchbase.mock.control.MockCommand;
import org.couchbase.mock.Info;

public class TimeTravelCommandHandler extends MockCommand {


    public TimeTravelCommandHandler(CouchbaseMock mock) {
        super(mock);
    }

    @Override
    public void execute(JsonObject payload, Command command) {
        if (payload.has("Offset")) {
            Info.timeTravel(payload.get("Offset").getAsInt());
        } else {
            throw new MissingRequiredFieldException("Offset");
        }
    }
}
