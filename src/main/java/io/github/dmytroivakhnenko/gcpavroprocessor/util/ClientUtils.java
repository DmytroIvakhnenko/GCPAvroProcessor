package io.github.dmytroivakhnenko.gcpavroprocessor.util;

import example.gcp.Client;
import example.gcp.ClientMandatory;

public class ClientUtils {
    public static ClientMandatory createMandatoryClientFromClient(Client client) {
        return new ClientMandatory(client.getId(), client.getName());
    }
}
