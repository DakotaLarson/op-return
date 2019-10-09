import { execFile } from "child_process";
import { existsSync, readFile, writeFile } from "fs";
import { Pool } from "pg";
import IOpReturnParent from "./IOpReturnParent";

export default class IndexGenerator {

    private static readonly RPC_USER = "dakota";
    private static readonly RPC_PASSWORD = "2yE4";

    private static readonly OP_RETURN_PREFIX = "6a";

    private static readonly EXE_PATH = "d:\\bitcoin\\daemon\\bitcoin-cli";
    private static readonly LAST_INDEXED_PATH = "./lastindexed";

    private static readonly MIN_BLOCK_INDEX = 500000;

    private lastIndexedBlockHeight: number;
    private maxIndexedBlockHeight: number;

    private pool: Pool;

    constructor(pool: Pool) {
        this.lastIndexedBlockHeight = 0;
        this.maxIndexedBlockHeight = 0;

        this.pool = pool;
    }

    public async index() {
        this.maxIndexedBlockHeight = await this.getBlockCount();
        this.lastIndexedBlockHeight = await this.getLastIndexedBlock();

        console.log(`Last indexed block height: ${this.lastIndexedBlockHeight}`);
        console.log(`Max indexed block height: ${this.maxIndexedBlockHeight}`);

        let nextIndexedBlockHash;
        while (this.lastIndexedBlockHeight < this.maxIndexedBlockHeight) {
            const results: Map<string, IOpReturnParent> = new Map();
            const nextIndexedBlockHeight = this.lastIndexedBlockHeight + 1;
            if (!nextIndexedBlockHash) {
                nextIndexedBlockHash = await this.getBlockHash(nextIndexedBlockHeight);
            }
            // @ts-ignore
            const block = await this.getBlock(nextIndexedBlockHash);

            for (const transaction of block.transactions) {
                const opReturn = await this.getTransactionOpReturn(transaction);
                if (opReturn) {
                    results.set(opReturn, {
                        transaction: transaction.hash,
                        block: nextIndexedBlockHash,
                    });
                }
            }
            console.log(`Height: ${nextIndexedBlockHeight}`);
            console.log(`Indexed ${results.size} / ${block.transactions.length}`);

            await this.save(results, nextIndexedBlockHeight, this.pool);

            nextIndexedBlockHash = block.nextBlockHash;
            this.lastIndexedBlockHeight = nextIndexedBlockHeight;
        }
    }

    private async save(results: Map<string, IOpReturnParent>, blockHeight: number, pool: Pool) {

        if (results.size) {
            this.updateDatabase(pool, results);
        }
        await this.updateLastIndexed(blockHeight);

    }

    private async updateDatabase(pgPool: Pool, results: Map<string, IOpReturnParent>) {
        let text = "INSERT INTO op_return (op_return, transaction_id, block_id) VALUES ";
        const values = [];
        let wildcardIndex = 0;
        for (const [opReturn, parent] of results) {
            text += `($${ ++ wildcardIndex }, $${ ++ wildcardIndex }, $${ ++ wildcardIndex }), `;
            values.push(opReturn, parent.transaction, parent.block);
        }
        text = text.substr(0, text.length - 2); // remove last comma + space

        await pgPool.query({
            text,
            values,
        });
    }

    private updateLastIndexed(blockHeight: number) {
        return new Promise((resolve, reject) => {
            writeFile(IndexGenerator.LAST_INDEXED_PATH, "" + blockHeight, (err) => {
                if (err) {
                    reject(err);
                } else {
                    resolve();
                }
            });
        });
    }

    private async getTransactionOpReturn(transaction: any) {
        for (const output of transaction.vout) {
            const hex = output.scriptPubKey.hex as string;
            if (hex.startsWith(IndexGenerator.OP_RETURN_PREFIX)) {
                return hex.substring(4);
            }
        }
        return undefined;
    }

    private async getBlock(blockHash: string) {
        const rawBlock = JSON.parse(await this.execute("getblock", [blockHash, "2"]));

        return {
            transactions: rawBlock.tx as string[],
            nextBlockHash: rawBlock.nextblockhash as string,
        };
    }

    private async getBlockHash(height: number) {
        return await this.execute("getblockhash", ["" + height]);
    }

    private getLastIndexedBlock(): Promise<number> {
        return new Promise((resolve, reject) => {
            if (existsSync(IndexGenerator.LAST_INDEXED_PATH)) {
                readFile(IndexGenerator.LAST_INDEXED_PATH, (err, data) => {
                    if (err) {
                        reject(err);
                    } else {
                        const lastIndexedBlock = parseInt(data.toString(), 10);
                        if (isNaN(lastIndexedBlock)) {
                            reject("Unexpected last indexed block: " + data.toString());
                        } else {
                            resolve(lastIndexedBlock);
                        }
                    }
                });
            } else {
                resolve(IndexGenerator.MIN_BLOCK_INDEX - 1);
            }
        });
    }

    private async getBlockCount() {
        const rawResult = await this.execute("getblockcount");
        const parsedResult = parseInt(rawResult, 10);
        if (isNaN(parsedResult)) {
            throw new Error("Unexpected result: " + rawResult);
        } else {
            return parsedResult;
        }
    }

    private execute(command: string, args?: string[]): Promise<string> {
        return new Promise((resolve, reject) => {
            const exeArguments = [
                "-rpcuser=" + IndexGenerator.RPC_USER,
                "-rpcpassword=" + IndexGenerator.RPC_PASSWORD,
                command,
            ];
            if (args) {
                for (const arg of args) {
                    exeArguments.push(arg);
                }
            }
            execFile(IndexGenerator.EXE_PATH, exeArguments, {
                maxBuffer: 1024 * 1024 * 1024,
            }, (err, result) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(result.trim());
                }
            });
        });
    }
}
