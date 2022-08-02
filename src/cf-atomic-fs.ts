
function toHexString(buffer: ArrayBuffer) { 
    return [...new Uint8Array(buffer)]
        .map(x => x.toString(16).padStart(2, '0'))
        .join('');
}

function log(s: any) {
    //console.log(s);
}


export class AtomicFileSystemDO {
    state:any;
    value:any|undefined;
	bucket:R2Bucket;
	
    constructor(state: any, env: any) {
        this.state = state;
		this.bucket = env.DB_BUCKET;

        this.state.blockConcurrencyWhile(async () => {
			let response = await this.bucket.get("ROOT");
			if (response) {
				this.value = await response.json();
				console.log(`DO INIT: ${this.value.version}`);
			}
/*
        	let value = await this.state.storage.get("value");
			if (value !== undefined) {
				this.value = JSON.parse(value);
				console.log(`DO Init: ${this.value.version}`)
			}*/
	    });
    }

    async fetch(request: Request) {
		let result = 200;
		if (request.method == "PUT") {
            let newValue:any = await request.json();
			if (!('version' in newValue)) {
				result = 400;
				console.log("DO: No version in new value");
			} else if (this.value === undefined || newValue.version == this.value.version) {
				newValue.version = crypto.randomUUID();
				console.log(`DO: new value: ${newValue.version}`);

				this.value = newValue;
				//await this.state.storage.put("value", this.value);
				this.bucket.put("ROOT", JSON.stringify(newValue));
			} else {
				result = 401;
				console.log("DO: Version changed, retry");

			}
        }
		if (this.value === undefined) {
			console.log("DO: value undefined");

			return new Response("{}", { status: 402 });
		}
        		
		return new Response(JSON.stringify(this.value), {
			headers: {
			  'content-type': 'application/json;charset=UTF-8',
			},
			status: result
		});
    }
}


export default function createFS(SQL: any, bucket: R2Bucket, atomicNS: DurableObjectNamespace, context: ExecutionContext) {

class FileHeader {
    blockShift = 18; // 256k
    blocks: string[] = [];
    version: string = "";
    super: string = "";
    superData: ArrayBuffer|undefined;

    get blockSize() { return 1<<this.blockShift; }
    get blockMask() { return this.blockSize - 1; }
}

class MyFile {
    // Name of the file
    filename: string;
    handle: number;
    // Lock level corresponds to SQL.LOCK
    lockLevel = SQL.LOCK_NONE;
    header: FileHeader = new FileHeader();
    // true if an atomic operation is in progress
    atomic = false;

    blocksToSync: { [id: number]: Uint8Array } = {};
    cachedBlocks: { [id: string]: Uint8Array } = {};

    constructor(filename: string, handle: number) {
        this.filename = filename;
        this.handle = handle;
    }

    // Open the file
    async open() {
        this.header = new FileHeader();

        let id = atomicNS.idFromName(this.filename);
        let stub = atomicNS.get(id);
        let response = await stub.fetch("http://example.com");
/*
        let response = await bucket.get(this.filename);
        if (response == null) {
            // OK, probably indicates file does not exist. Will create the file
            console.log(`open(${this.filename}) failed.`);
            return;
        }
*/
        if (response.status == 200) {
            let data: any = await response.json();
            this.header.blockShift = data.blockShift;
            this.header.version = data.version;
            this.header.blocks = data.blocks;
            this.header.super = data.super;
            log(`open(${this.filename}): ${data.version} ${JSON.stringify(data)}`);
        } else {
            log(`open(${this.filename}): ${response.status}`);
        }
        /*
        if (this.header.super != "") {
            let r2 = await this.getCachedBlock(data.super);
            this.header.superData = await r2.arrayBuffer();
        }
        */
    }

    // Gets the size of the file
    size() {
        return this.header.blocks.length * this.header.blockSize;
    }

    async putCachedBlock(blockId: string, data: ArrayBuffer) {
        const cache = caches.default;

        const cacheKey = new Request(`http://example.com/2/block/${blockId}`);

        let block = await bucket.put(blockId, data, {
            httpMetadata: new Headers({
                'content-type': 'application/octet-stream',
                'Cache-Control': 'public, max-age=604800, immutable',
              }),
        });
  
        const headers = new Headers();
        block.writeHttpMetadata(headers);
        headers.set('etag', block.httpEtag);

        // Cache API respects Cache-Control headers. Setting s-max-age to 10
        // will limit the response to be in cache for 604800 seconds max
        // Any changes made to the response here will be reflected in the cached value
        headers.append('Cache-Control', 'public, s-maxage=604800, immutable');
        
        let response = new Response(data, {
            headers,
        });

        // Store the fetched response as cacheKey
        // Use waitUntil so you can return the response without blocking on
        // writing to cache
        context.waitUntil(cache.put(cacheKey, response));

        return block;
    }

    async getCachedBlock(blockId: string) {
        const cache = caches.default;

        let cacheKey = `http://example.com/3/block/${blockId}`;
        let cacheReq = new Request(cacheKey);
        let response = await cache.match(cacheReq);
        if (response) {
            return response;
        }
        
        let offset = blockId.indexOf("/");
        let range: R2Range|undefined;
        if (offset != -1) {
            range = {
                offset: parseInt(blockId.substring(offset+1)),
                length: this.header.blockSize
            };
            blockId = blockId.substring(0, offset);
        }
        
        let block = await bucket.get(blockId, { range: range }) as R2ObjectBody;
        if (block === null) {
            console.log()
            return new Response('Block Not Found', { status: 404 });
        }
        
        const headers = new Headers();
        block.writeHttpMetadata(headers);
        headers.set('etag', block.etag);

        // Cache API respects Cache-Control headers. Setting s-max-age to 10
        // will limit the response to be in cache for 604800 seconds max
        // Any changes made to the response here will be reflected in the cached value
        headers.append('Cache-Control', 'public, s-maxage=604800, immutable');
        
        response = new Response(block.body, {
            headers,
        });
        // Store the fetched response as cacheKey
        // Use waitUntil so you can return the response without blocking on
        // writing to cache
        context.waitUntil(cache.put(cacheReq, response.clone()));
        return response;
    }

    // Reads a block from the file
    async readBlock(offset: number, length: number|null = null) {
        length = length || this.header.blockSize;

        let writtenBlock = this.blocksToSync[offset >> this.header.blockShift];
        if (writtenBlock !== undefined) {
            let end = Math.min(writtenBlock.byteLength, (offset & this.header.blockMask) + length);
            log(`readBlock([CACHED], ${offset}, ${length}, ${end}) = ${writtenBlock.byteLength}`);
            return writtenBlock.subarray(offset & this.header.blockMask, end);
        }

        let blockId = this.header.blocks[offset >> this.header.blockShift];
        if (blockId == "") {
            blockId = `${this.header.super}/${offset & ~this.header.blockMask}`;
        }
        
        if (blockId != "") {
            let cachedBlock = this.cachedBlocks[blockId];
            if (cachedBlock != undefined) {
                let end = Math.min(cachedBlock.byteLength, (offset & this.header.blockMask) + length);
                log(`readBlock(${blockId} [CACHED], ${offset}, ${length}, ${end}) = ${cachedBlock.byteLength}`);
                return cachedBlock.subarray(offset & this.header.blockMask, end);
            }
            log(`readBlock(${blockId}, ${offset}, ${length})`);
            let block = await this.getCachedBlock(blockId);
            if (block === null) {
                console.error(`readBlock(${blockId}, ${offset}, ${length}) was null`);
                return null;
            }
            const buffer = new Uint8Array(await block.arrayBuffer());
            this.cachedBlocks[blockId] = buffer;
            let end = Math.min(buffer.byteLength, (offset & this.header.blockMask) + length);
            log(`readBlock(${blockId}, ${offset}, ${length}, ${end}) = ${buffer.byteLength}`);
            return buffer.subarray(offset & this.header.blockMask, end);
        } /*
        else {
            if (this.header.superData !== undefined) {
                log(`readBlock(SUPER (cached), ${offset}, ${length})`);
                return new Uint8Array(this.header.superData, offset, length);
            }

            let block = await this.getCachedBlock(this.header.super, { offset: offset & ~this.header.blockMask, length: this.header.blockSize });
            if (block === null) {
                console.error(`readBlock(${blockId}, ${offset}, ${length}) was null`);
                return null;
            }
            const buffer = new Uint8Array(await block.arrayBuffer());
            //this.cachedBlocks[`${this.header.super}`] = buffer;
            
            let end = Math.min(buffer.byteLength, (offset & this.header.blockMask) + length);
            log(`readBlock(${this.header.super}, ${offset}, ${length}, ${end}) = ${buffer.byteLength}`);
            return buffer.subarray(offset & this.header.blockMask, end);
        } */
    }

    async writeBlock(buffer: Uint8Array, offset: number) {
        if (!this.atomic) {
            console.error("Atomic writes have not begin");
            return SQL.ABORT;
        }

        if (buffer.length != this.header.blockSize || (offset & this.header.blockMask) != 0) {
            // Unaligned read...
            let block = await this.readBlock(offset & ~this.header.blockMask, this.header.blockSize);
            if (block == null) {
                block = new Uint8Array(this.header.blockSize);
            }
            block.set(buffer, offset & this.header.blockMask);
            buffer = block;
        }

        this.blocksToSync[offset >> this.header.blockShift] = buffer;

        return SQL.OK;
    }

    async atomicCommit() {
        await Promise.all(Object.entries(this.blocksToSync).map(async x => {
            let blockIndex = parseInt(x[0]);
            let block = x[1];
            const blockId = toHexString(await crypto.subtle.digest('md5', block));
            log(`flushWrite(${blockIndex}, ${blockId})`);
            this.header.blocks[blockIndex] = blockId;
            await this.putCachedBlock(blockId, block);
            this.cachedBlocks[blockId] = block;
        }));
        this.blocksToSync = {};

        console.log(`commit: ${this.header.blocks.length} * ${this.header.blockShift}`)
        const json = JSON.stringify({ 
            blockShift: this.header.blockShift,
            blocks: this.header.blocks,
            super: this.header.super,
            version: this.header.version
        });

        let id = atomicNS.idFromName(this.filename);
        let stub = atomicNS.get(id);
        let request = new Request("http://example.com", { method: "PUT", body: json });
        let response = await stub.fetch(request);

        let newResponse:any = await response.json();
        log(`atomicCommit(${this.filename}, ${this.header.version}) ${this.header.version} -> ${newResponse.version} = "${json}" ${response.status}`);
        this.header.version = newResponse.version;
        this.atomic = false;
        return response.status == 200 ? SQL.OK : SQL.BUSY;
    }

    async atomicRollback() {
        this.atomic = false;
        await this.open();
    }
}

// In-memory (atomic) filesystem
class MyVfs {
    // Map open file handles to file
    fileHandles: { [id: number]: MyFile } = {};
    // Handle allocator
    next_handle = 1;

    // Debug: Map integer flags to strings
    _flags(prefix: string, flags: number) {
        let flagDesc = [];
        for (const key of Object.keys(SQL)) {
            if (key.startsWith(prefix) && (flags & SQL[key]) != 0) {
                flagDesc.push(key);
            }
        }
        return flagDesc.join("|");
    }

    // Debug: Map an enum value to string
    _enum(prefix: string, value: number) {
        for (const key of Object.keys(SQL)) {
            if (key.startsWith(prefix) && SQL[key] === value) {
                return key;
            }
        }

        return undefined;
    }

    // Open a file
    async open(filename: string, flags: number) {
        let handle = this.next_handle++;
        let flagDesc = this._flags("OPEN_", flags);
        log(`open(${filename}, ${flags} ${flagDesc}) = ${handle}`);
        
        let file = new MyFile(filename, handle);
        await file.open();
        this.fileHandles[handle] = file;
        return [SQL.OK, handle, 0];
    }

    // Test access to a file
    async access(filename: string, flags: number) {
        let result = false;
        if (flags == SQL.ACCESS_EXISTS || flags == SQL.ACCESS_READWRITE || flags == SQL.ACCESS_READ) {
            if (filename.indexOf("-wal") == -1 && filename.indexOf("-journal") == -1) {
                let response = await bucket.head(filename);
                result = response != null;
            }
        }

        let flagDesc = this._enum("ACCESS_", flags);
        log(`access(${filename}, ${flags} ${flagDesc}) = ${result}`);
        return [SQL.OK, result];
    }

    // Delete a file
    async delete(filename: string, syncDir: number) {
        log(`delete(${filename}, ${syncDir})`);
        let [result, fileHandle, flags] = await this.open(filename, 0);
        let file = this.fileHandles[fileHandle];

        await Promise.all(file.header.blocks.filter(blockId => blockId != "")
            .map(blockId => bucket.delete(blockId)));
        await bucket.delete(filename);
        return SQL.OK;
    }

    // Close a file
    async fileClose(file: number) {
        delete this.fileHandles[file];
        return SQL.OK;
    }

    // Sync a file. Ensure all changes are written to permanent storage
    async fileSync(file: number, flags: number) {
        return SQL.OK;
    }

    // Returns the size of a file
    async fileSize(file: number|string) {
        if (typeof file === "number") {
            let size = this.fileHandles[file].size();
            return [SQL.OK, size];
        } else {
            let response = await bucket.head(file);
            let size = response?.customMetadata.fileSize;
            if (size !== undefined) {
                log(`fileSize(${file}) = ${size}`);
                return [SQL.OK, parseInt(size)];
            }
        }

        return [SQL.ERROR, 0];
    }

    // Read data from a file
    async fileRead(fileHandle: number, buffer: Uint8Array, offset: number) {
        let file = this.fileHandles[fileHandle];
        log(`fileRead(${fileHandle}, ${buffer.byteLength}, ${offset})`);

        // If short read: memset remaining bytes read to 0 and return SQL.IOERR_SHORT_READ
        buffer.fill(0);

        if (offset < file.size()) {
            const readLength = Math.min(file.size() - offset, buffer.length);
            let bytesRead = 0;
            while (bytesRead < readLength) {
                let data = await file.readBlock(offset + bytesRead, readLength - bytesRead);
                if (data == null) return SQL.IOERR_READ;

                buffer.set(new Uint8Array(data), bytesRead);
                bytesRead += data.byteLength;
            }
            
            return bytesRead == buffer.length ? SQL.OK : SQL.IOERR_SHORT_READ;
        }

        // If failed, return SQL.IOERR_CORRUPTFS or SQL.IOERR_READ				
        return SQL.IOERR_SHORT_READ;
    }

    // Write data to a file
    async fileWrite(fileHandle: number, buffer: Uint8Array, offset: number) {
        log(`write(${fileHandle}, ${buffer.byteLength}, ${offset})`);
        let file = this.fileHandles[fileHandle];

        if (file.atomic) {
            return await file.writeBlock(buffer, offset);
        }
        return SQL.BUSY; // only atomic supported! Force a retry.
    }

    // Get device characteristics
    async fileDeviceCharacteristics(file: number) {
        return SQL.IOCAP_BATCH_ATOMIC;
    }

    // Get file sector size
    async fileSectorSize(file: number) {
        return this.fileHandles[file].header.blockSize;
    }

    // Extended control of a file
    async fileControl(fileHandle: number, op: number, arg: any) {
        let opDesc = this._enum("FCNTL_", op);
        log(`control(${fileHandle}, ${opDesc} (${op}), ${arg})`);

        let file = this.fileHandles[fileHandle];
        switch (op) {
            // Begin an atomic write
            case SQL.FCNTL_BEGIN_ATOMIC_WRITE:
                file.atomic = true;
                return SQL.OK;
            // Commit an atomic write
            case SQL.FCNTL_COMMIT_ATOMIC_WRITE:
                return await file.atomicCommit();
            // Rollback an atomic write
            case SQL.FCNTL_ROLLBACK_ATOMIC_WRITE:
                await file.atomicRollback();
                return SQL.OK;
            case SQL.FCNTL_PRAGMA:
                let [_, name, value] = arg;
                if (name == "journal_mode" && value.toUpperCase() != "MEMORY") {
                    console.error("PRAGMA journal_mode = MEMORY is required for atomic commit");
                    return SQL.ABORT;
                }
                break;
            default:
                break;
        }	

        return SQL.NOTFOUND;
    }

    // Increase the lock level on the file
    async fileLock(fileHandle: number, lockLevel: number) {
        let file = this.fileHandles[fileHandle];
        let oldLockDesc = this._enum("LOCK_", file.lockLevel);
        let newLockDesc = this._enum("LOCK_", lockLevel);
        log(`lock(${fileHandle}, ${oldLockDesc} (${file.lockLevel}) -> ${newLockDesc} (${lockLevel}))`);
        file.lockLevel = lockLevel;
        if (lockLevel == SQL.LOCK_SHARED) {
            await file.open();
        }
        return SQL.OK;
    }

    // Reduce the lock level on a file
    async fileUnlock(fileHandle: number, lockLevel: number) {
        let file = this.fileHandles[fileHandle];
        let oldLockDesc = this._enum("LOCK_", file.lockLevel);
        let newLockDesc = this._enum("LOCK_", lockLevel);
        log(`unlock(${fileHandle}, ${oldLockDesc} (${file.lockLevel}) -> ${newLockDesc} (${lockLevel}))`);
        file.lockLevel = lockLevel;

        return SQL.OK;
    }

    // Check the lock level on a file
    async fileCheckReservedLock(file: number) {
        let lockLevel = this.fileHandles[file].lockLevel;
        let desc = this._enum("LOCK_", lockLevel);
        log(`checkReservedLock(${file}) = ${desc} (${lockLevel})`);

        return [SQL.OK, lockLevel];
    }

    // Truncate a file
    async fileTruncate(fileHandle: number, newSize: number) {
        //
        let file = this.fileHandles[fileHandle];
        return SQL.OK;
    }

    async download(filename: string, stream: WritableStream) {
        log(`download(${filename})`);
        
        const file = new MyFile(filename, this.next_handle++);
        await file.open();
        
        let offset = 0;
        for (const blockId of file.header.blocks) {
            let block = blockId != "" ? await bucket.get(blockId)
                : await bucket.get(file.header.super, { range: { offset: offset, length: file.header.blockSize }}) as R2ObjectBody;
            if (block == null) {
                throw Error(`reading block failed!`);
            }

            await block.body.pipeTo(stream, { preventClose: true });
            offset += file.header.blockSize;
        }
    }

    async upload(filename: string, stream: ArrayBuffer) {
        console.log(`upload(${filename})`);
        const file = new MyFile(filename, this.next_handle++);
        await file.open();
        file.atomic = true;
    
        const superId = `${filename}.SUPER`; //toHexString(await crypto.subtle.digest('md5', stream));
        let r = await bucket.put(superId, stream, {
            httpMetadata: new Headers({
                'content-type': 'application/octet-stream',
                'Cache-Control': 'public, max-age=604800',
              }),
        });

        let blockCount = (stream.byteLength + file.header.blockSize - 1) >> file.header.blockShift;
        log(`upload(${superId}) = ${r.version} blocks=${blockCount} size=${stream.byteLength}`);

        file.header.super = superId;
        file.header.blocks = Array(blockCount);
        file.header.blocks.fill("");
       
        await file.atomicCommit();
    }
}

    return new MyVfs();
}