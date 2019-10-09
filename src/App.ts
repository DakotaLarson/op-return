import { Pool } from "pg";
import IndexGenerator from "./IndexGenerator";
import WebServer from "./WebServer";

class App {

    private static readonly PG_HOST = "localhost";
    private static readonly PG_USER = "api_user";
    private static readonly PG_PASSWORD = "fiESk1FF6^y749HI";
    private static readonly PG_DATABASE = "op_return";
    private static readonly PG_PORT = 5432;

    private static readonly INDEX_COOLDOWN = 6000;

    public async start() {
        const pgPool = new Pool({
            host: App.PG_HOST,
            port: App.PG_PORT,
            user: App.PG_USER,
            password: App.PG_PASSWORD,
            database: App.PG_DATABASE,
        });
        await pgPool.connect();
        console.log("connected");

        const server = new WebServer(pgPool);
        server.start();

        this.index(pgPool);
    }

    private async index(pool: Pool) {
        const indexGenerator = new IndexGenerator(pool);
        await indexGenerator.index();
        setInterval(async () => {
            await indexGenerator.index();
        }, App.INDEX_COOLDOWN);
    }
}

(() => {
    const app = new App();
    app.start();
})();
