package com.jpmc.midascore.foundation;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import com.jpmc.midascore.service.IncentiveService;
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

    @Autowired
    private IncentiveService incentiveService;

    @KafkaListener(topics = "${general.kafka-topic}")
    @Transactional
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        // Find sender and recipient
        UserRecord sender = userRepository.findById(transaction.getSenderId()).orElse(null);
        UserRecord recipient = userRepository.findById(transaction.getRecipientId()).orElse(null);

        // Validate transaction
        if (!isValidTransaction(transaction, sender, recipient)) {
            logger.warn("Invalid transaction: {}", transaction);
            return;
        }

        // Process transaction
        processTransaction(transaction, sender, recipient);

        // Debug: Log wilbur's balance after each transaction
        UserRecord wilbur = userRepository.findByName("wilbur");
        if (wilbur != null) {
            logger.info("DEBUG - Wilbur's current balance: {}", wilbur.getBalance());
        }
    }

    private boolean isValidTransaction(Transaction transaction, UserRecord sender, UserRecord recipient) {
        // Check if sender and recipient exist
        if (sender == null || recipient == null) {
            logger.warn("Sender or recipient not found");
            return false;
        }

        // Check if sender has sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Insufficient balance for sender: {}", sender.getName());
            return false;
        }

        return true;
    }

    private void processTransaction(Transaction transaction, UserRecord sender, UserRecord recipient) {
        // Get incentive amount
        float incentiveAmount = incentiveService.getIncentiveAmount(transaction);
        logger.info("Received incentive amount: {}", incentiveAmount);

        // Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentiveAmount);

        // Save updated user records
        userRepository.save(sender);
        userRepository.save(recipient);

        // Create and save transaction record
        TransactionRecord transactionRecord = new TransactionRecord(sender, recipient, transaction.getAmount(), incentiveAmount);
        transactionRepository.save(transactionRecord);

        logger.info("Transaction processed successfully: {}", transactionRecord);
    }
} 