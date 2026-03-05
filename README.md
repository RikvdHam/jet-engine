# Transactions System

The **Transactions System** is a Python Package created to train new employees
in Python and OOP.

## Scenario:
You are building a mini banking system that supports multiple types of accounts
(e.g., Checkings, Savings, Business). Each account type has a different rules
for transactions, interest, and fees.

Each account supports two simple type of transactions: deposit and withdrawal.
Each account also has a function to apply interest. 
An account can never have a negative balance. 
There are three types of accounts:

Checking Account
1. Every withdrawal has a fee of 1 dollar
2. Interest rate of the checkings account is 1%
3. Withdrawal limit: 5,000

Savings Account
1. No fees for withdrawal
2. Interest rate of 2.5%
3. Possible initial balance, which is bank dependent
4. Withdrawal limit: 2,500

Business Account
1. No fees for withdrawal
2. No interest rate
3. Deposit bonus of 0.5% on the deposited amount
4. Withdrawal limit: 50,000

Each account has an unique account number, account holder and keeps track of the 
total balance and number of transactions done on the account.

The system should include a Bank (e.g., "Bank A") that can register and 
deregister accounts. Using the bank, it should be possible to perform deposits 
and withdrawals, by specifying the account number. The bank should also provide 
a method to apply interest to all registered accounts.

## Addiional features:
1. Implement transfers between accounts of the same bank.
2. Store transaction history inside the Bank's memory. This includes the type of
transaction (deposit, withdrawal, transfer) and the possible type of rejection 
(insuffient funds, invalid account, withdrawal limits). Use enums for types.
3. Create a function that prints the number of suspious transactions (larger
than 10,000) and the percentage of rejected vs total transactions.
4. Use a decorator to log (print) the type of bank account, the account number,
the account holder and the type of transactions that is tried to be done. After
completion of the transaction, log the same including the new balance.
5. Build an account statement function that prints the transaction history of
a certain bank account.