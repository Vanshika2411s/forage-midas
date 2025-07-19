package com.jpmc.midascore.foundation;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import org.springframework.beans.factory.annotation.Autowired;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.entity.UserRecord;


@Component
public class DataInitializer implements CommandLineRunner {

    @Autowired
    private UserRepository userRepository;

    @Override
    public void run(String... args) {
        userRepository.save(new UserRecord("Alice", 1000.0));
        userRepository.save(new UserRecord("Bob", 500.0));
        userRepository.save(new UserRecord("Charlie", 750.0));
        userRepository.save(new UserRecord("Diana", 300.0));
        userRepository.save(new UserRecord("Eve", 200.0));
        userRepository.save(new UserRecord("Frank", 900.0));
        userRepository.save(new UserRecord("Grace", 600.0));
        userRepository.save(new UserRecord("Hank", 100.0));
        userRepository.save(new UserRecord("Ivy", 150.0));
        userRepository.save(new UserRecord("Jack", 250.0));

        System.out.println("✅ Dummy users inserted into H2 database.");
    }
}
