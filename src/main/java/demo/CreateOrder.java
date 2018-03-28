package demo;

import lombok.Data;

import java.util.Date;

@Data
public class CreateOrder implements MyEventType {

    String eventName = this.getClass().getSimpleName();

    Date date;

    User user;

    @Data
    public static class User {

        String id;

        String firstName;

        String lastName;
    }
}
