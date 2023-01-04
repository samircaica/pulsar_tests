package org.interacting.bookkeeper;

import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Enumeration;

public class BkClient {

    public static BookKeeper createClient() {
        try {
            String connectionString = "127.0.0.1:2181"; // For a single-node, local ZooKeeper cluster
            BookKeeper bkClient = new BookKeeper(connectionString);
            return bkClient;
        } catch (InterruptedException | IOException | BKException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) throws BKException, InterruptedException {
        BookKeeper client = createClient();
        byte[] ledgerPassword = "some-password".getBytes();

        LedgerHandle lh = client.createLedger(BookKeeper.DigestType.MAC, ledgerPassword);
        long ledgerId = lh.getId();

        ByteBuffer entry = ByteBuffer.allocate(4);

        int numberOfEntries = 100;

        for (int i = 0; i < numberOfEntries; i++) {
            entry.putInt(i);
            entry.position(0);
            lh.addEntry(entry.array());
        }
        lh.close();

        lh = client.openLedger(ledgerId, BookKeeper.DigestType.MAC, ledgerPassword);

        Enumeration<LedgerEntry> entries = lh.readEntries(0, numberOfEntries - 1);

        while(entries.hasMoreElements()) {
            ByteBuffer result = ByteBuffer.wrap(entries.nextElement().getEntry());
            Integer retrEntry = result.getInt();

            // Print the integer stored in each entry
            System.out.println(String.format("Result: %s", retrEntry));
        }

        // Close the ledger and the client
        lh.close();
        client.close();
    }

}
