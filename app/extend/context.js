const TRANSACTION_CLIENTS = Symbol.for('egg-dao#transactionClients');

module.exports = {
    async transaction(fn) {
        if (this[TRANSACTION_CLIENTS]) {
            return fn();
        }
        this[TRANSACTION_CLIENTS] = [];
        try {
            await fn();
            await Promise.all(this[TRANSACTION_CLIENTS].map(client => {
                return client.connection.commit();
            }));
        } catch (error) {
            await Promise.all(this[TRANSACTION_CLIENTS].map(client => {
                return client.connection.rollback();
            }));
        }
        this[TRANSACTION_CLIENTS] = null;
    }
};