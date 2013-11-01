package org.couchbase.mock.control.handlers;

import com.google.gson.JsonObject;
import org.couchbase.mock.CouchbaseMock;
import org.couchbase.mock.control.MissingRequiredFieldException;
import org.couchbase.mock.control.MockCommand;
import org.couchbase.mock.Info;
import org.couchbase.mock.control.CommandStatus;
import org.jetbrains.annotations.NotNull;

public final class TimeTravelCommandHandler extends MockCommand {
    @NotNull
    @Override
    public CommandStatus execute(@NotNull CouchbaseMock mock, @NotNull Command command, @NotNull JsonObject payload) {
        if (payload.has("Offset")) {
            Info.timeTravel(payload.get("Offset").getAsInt());
        } else {
            throw new MissingRequiredFieldException("Offset");
        }
        return getResponse();
    }
}
