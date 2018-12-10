package ru.innopolis.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.transactions.Transaction;

public class Simple {
    public static void main(String[] args) {
        Ignition.start();
        //putAndGetCache();
        transactionalCache(1);
        Ignition.stop(true);
    }

    public static void putAndGetCache() {
        Ignite ignite = Ignition.ignite();
        final IgniteCache<Integer, String> cache = ignite.getOrCreateCache("stc12");
        for (int i = 0; i < 10; i++) {
            cache.put(i, Integer.toString(i));
        }
        for (int i = 0; i < 10; i++) {
            System.out.println("got key=" + i + ", value=" + cache.get(i));
        }
        for (int i = 0; i < 10; i++) {
            cache.remove(i);
        }
        System.out.println("after deletion:");
        for (int i = 0; i < 10; i++) {
            System.out.println("got key=" + i + ", value=" + cache.get(i));
        }
    }

    private static void transactionalCache(Integer acctId) {
        Ignite ignite = Ignition.ignite();
        Account account = new Account(acctId, 23.30D);
        IgniteCache<Integer, Account> cache = ignite.getOrCreateCache("STC12");
        cache.put(acctId, account);
        try (Transaction tx = ignite.transactions().txStart()) {
            Account account1 = cache.get(acctId);
            assert account1 != null;
            account1.setBalance(account1.getBalance() + 20);
            cache.put(acctId, account1);
            tx.rollback();
        }
        System.out.println(cache.get(acctId));
    }
}
