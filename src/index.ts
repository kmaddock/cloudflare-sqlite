/**
 * Welcome to Cloudflare Workers! This is your first worker.
 *
 * - Run `wrangler dev src/index.ts` in your terminal to start a development server
 * - Open a browser tab at http://localhost:8787/ to see your worker in action
 * - Run `wrangler publish src/index.ts --name my-worker` to publish your worker
 *
 * Learn more at https://developers.cloudflare.com/workers/
 */

export interface Env {
	// Example binding to KV. Learn more at https://developers.cloudflare.com/workers/runtime-apis/kv/
	// MY_KV_NAMESPACE: KVNamespace;
	//
	// Example binding to Durable Object. Learn more at https://developers.cloudflare.com/workers/runtime-apis/durable-objects/
	// MY_DURABLE_OBJECT: DurableObjectNamespace;
	//
	// Example binding to R2. Learn more at https://developers.cloudflare.com/workers/runtime-apis/r2/
	DB_BUCKET: R2Bucket;
	DB_ATOMIC: DurableObjectNamespace;
}

import initSQL from "./sql.js";
import createFS from "./cf-atomic-fs.js";
export { AtomicFileSystemDO } from "./cf-atomic-fs.js";


export default {
	async fetch(
		request: Request,
		env: Env,
		ctx: ExecutionContext
		): Promise<Response> {
		const url = new URL(request.url);
		/*
		if (SQL instanceof Promise) {
			console.log("Waiting for SQL module...");
			SQL = await SQL;
		}*/
		let SQL = await initSQL();
		const vfs = createFS(SQL, env.DB_BUCKET, env.DB_ATOMIC, ctx);
		SQL.registerVfs(vfs);

		if (url.pathname == "/query") {
			let query = await request.text();
			
			let db = new SQL.Database();
			db.filename = "ROOT";
			await db.open();
			// Setup initial pragmas
			await db.exec(`PRAGMA journal_mode = MEMORY; -- Required for batch atomic write
PRAGMA page_size = 16384;
PRAGMA synchronous = normal; -- synchronise less often to the filesystem
PRAGMA foreign_keys = on; -- check foreign key reference, slightly worse performance
`);

			try {
				const result = await db.execRetry(query);
				
				return new Response(JSON.stringify(result), {
					headers: {
					  'content-type': 'application/json;charset=UTF-8',
					},
				});
			} catch (e: any) {
				console.log(`Caught exception: ${e}`);
				return new Response(JSON.stringify({
					success: false,
					message: e.toString()
				}), {
					headers: {
						'content-type': 'application/json;charset=UTF-8',
					  },
					status: 400
				  });	
			}
		} else if (url.pathname == "/download") {
			let [_, fileSize] = await vfs.fileSize("ROOT");
			let { readable, writable } = new FixedLengthStream(fileSize);

			vfs.download("ROOT", writable);
			return new Response(readable);
		} else if (url.pathname == "/upload") {
			if (request.body != null) {
				await vfs.upload("ROOT", await request.arrayBuffer());
				return new Response("ok then");
			}
		} else if (url.pathname == "/delete") {
			await vfs.delete("ROOT", 0);
		} else if (url.pathname == "/clean") {
			while (true) {
				let response = await env.DB_BUCKET.list();
				console.log(response.objects.map(obj => obj.key));
				await Promise.all(response.objects.map(obj => env.DB_BUCKET.delete(obj.key)));
				if (response.cursor == null) {
					break;
				}
			}
			return new Response("wasted the bucket");

		}
		return new Response("Hello World!");
	},
};
