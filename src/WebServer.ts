import * as Express from "express";
import { Pool } from "pg";

export default class WebServer {

    private static readonly PORT = 8000;
    private static readonly PATH_PREFIX = "/opreturn/";

    private pool: Pool;

    constructor(pool: Pool) {
        this.pool = pool;
    }

    public start() {
        const app = Express();
        app.get("/opreturn/*", this.handleRequest.bind(this));

        app.listen(WebServer.PORT);
    }

    private async handleRequest(req: Express.Request, res: Express.Response) {
        const opReturn = req.path.replace(WebServer.PATH_PREFIX, "");
        if (opReturn) {
            const results = await this.getOpReturnData(opReturn);
            res.status(200).set({
                "content-type": "application/json",
            }).send(results);
        }
    }

    private async getOpReturnData(opReturn: string) {
        const text = "SELECT transaction_id, block_id FROM op_return WHERE op_return = $1";
        const values = [opReturn];

        const rawResults = await this.pool.query({
            text,
            values,
        });

        const results = [];
        for (const row of rawResults.rows) {
            results.push({
                transaction: row.transaction_id,
                block: row.block_id,
            });
        }

        return results;
    }
}
