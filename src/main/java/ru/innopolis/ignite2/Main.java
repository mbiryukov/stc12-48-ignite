package ru.innopolis.ignite2;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import ru.innopolis.ignite2.ServiceGrid.User;
import ru.innopolis.ignite2.ServiceGrid.UserService;
import ru.innopolis.ignite2.ServiceGrid.UserServiceImpl;

public class Main {
    public static void main(String[] args) {
/*        Ignition.start();
        try {
            Class.forName("org.apache.ignite.IgniteJdbcThinDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        SqlApache sqlApache= null;
        sqlApache = new SqlApache();
        assert sqlApache!=null;
        sqlApache.insertData();
        sqlApache.getData();
        Ignition.stop(true);*/
        startUserService();
    }

    public static void startUserService() {
        try (Ignite ignite = Ignition.start()) {
            ignite.services().deployClusterSingleton("UserServiceImpl", new UserServiceImpl());
            UserService userService = ignite.services().serviceProxy("UserServiceImpl", UserService.class, false);
            User user = new User("user", "12345");
            userService.addUser("1", user);
            user = new User("admin", "12321");
            userService.addUser("2", user);
            System.out.println("Check user 1: " + userService.checkUser("1"));
            System.out.println("Get user 2: " + userService.getUser("2"));
        }
    }
}
