package com.jpmc.midascore.foundation;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class TransactionListener {

    @Autowired
    private UserRepository userRepository;

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-core-group")
    @Transactional
    public void listen(Transaction transaction) {
        System.out.println("✅ Received transaction: " + transaction);

        // Fetch users
        //UserRecord sender = userRepository.findById(senderId).orElse(null);

        //UserRecord sender = userRepository.findById(transaction.getSenderId());
        //UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        UserRecord sender = userRepository.findById(transaction.getSenderId()).orElse(null);
        UserRecord recipient = userRepository.findById(transaction.getRecipientId()).orElse(null);

        // Basic validations
        if (sender == null || recipient == null) {
            System.out.println("❌ Sender or recipient not found.");
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            System.out.println("❌ Sender has insufficient balance.");
            return;
        }

        // Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        // Save back to DB
        userRepository.save(sender);
        userRepository.save(recipient);

        System.out.println("✅ Transaction applied successfully.");
    }
}
