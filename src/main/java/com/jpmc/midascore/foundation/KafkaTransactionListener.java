package com.jpmc.midascore.foundation;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
public class KafkaTransactionListener {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTransactionListener.class);

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private TransactionRepository transactionRepository;

    @KafkaListener(topics = "${general.kafka-topic}")
    @Transactional
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        UserRecord sender = userRepository.findById(transaction.getSenderId()).orElse(null);
        UserRecord recipient = userRepository.findById(transaction.getRecipientId()).orElse(null);

        if (!isValidTransaction(transaction, sender, recipient)) {
            logger.warn("Invalid transaction: {}", transaction);
            return;
        }

        // Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        // Save changes
        userRepository.save(sender);
        userRepository.save(recipient);

        // Record transaction
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount());
        transactionRepository.save(transactionRecord);

        // Log Waldorf's balance if exists
        UserRecord waldorf = userRepository.findByName("waldorf");
        if (waldorf != null) {
            logger.info("DEBUG - Waldorf's current balance: {}", waldorf.getBalance());
        } else {
            logger.warn("DEBUG - Waldorf not found in database");
        }
    }

    private boolean isValidTransaction(Transaction transaction, UserRecord sender, UserRecord recipient) {
        if (sender == null || recipient == null) {
            logger.warn("Sender or recipient not found");
            return false;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Insufficient balance for sender: {}", sender.getName());
            return false;
        }

        return true;
    }
}
