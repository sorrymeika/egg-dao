const { BaseContextClass } = require('egg');
const Connection = require('./connection');

class Dao extends BaseContextClass {
    static clientName() {
        return null;
    }

    constructor(ctx) {
        super(ctx);

        const clientName = this.constructor.clientName(ctx);
        this.connection = new Connection(ctx, clientName);
    }
}

module.exports = Dao;