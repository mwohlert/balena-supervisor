import { ClientRequest } from 'http';
import * as https from 'https';
import * as _ from 'lodash';
import * as stream from 'stream';
import * as url from 'url';
import * as zlib from 'zlib';

import { LogBackend, LogMessage } from './log-backend';

import log from '../lib/supervisor-console';

const ZLIB_TIMEOUT = 100;
const COOLDOWN_PERIOD = 5 * 1000;
const KEEPALIVE_TIMEOUT = 60 * 1000;
const RESPONSE_GRACE_PERIOD = 5 * 1000;

const MAX_LOG_LENGTH = 10 * 1000;
const MAX_PENDING_BYTES = 256 * 1024;

interface Options extends url.UrlWithParsedQuery {
	method: string;
	headers: Dictionary<string>;
}

export class BalenaLogBackend extends LogBackend {
	private req: ClientRequest | null = null;
	private dropCount: number = 0;
	private writable: boolean = true;
	private gzip: zlib.Gzip | null = null;
	private opts: Options;
	private stream: stream.PassThrough;
	private timeout: NodeJS.Timer;

	public initialised = false;

	public constructor(
		apiEndpoint: string,
		uuid: Nullable<string>,
		deviceApiKey: string,
	) {
		super();

		if (uuid != null && deviceApiKey !== '') {
			this.assignFields(apiEndpoint, uuid, deviceApiKey);
		}
		// This stream serves serves as a message buffer during reconnections
		// while we unpipe the old, malfunctioning connection and then repipe a
		// new one.
		this.stream = new stream.PassThrough({
			allowHalfOpen: true,

			// We halve the high watermark because a passthrough stream has two
			// buffers, one for the writable and one for the readable side. The
			// write() call only returns false when both buffers are full.
			highWaterMark: MAX_PENDING_BYTES / 2,
		});

		this.stream.on('drain', () => {
			this.writable = true;
			this.flush();
			if (this.dropCount > 0) {
				this.write({
					message: `Warning: Suppressed ${
						this.dropCount
					} message(s) due to high load`,
					timestamp: Date.now(),
					isSystem: true,
					isStdErr: true,
				});
				this.dropCount = 0;
			}
		});
	}

	public log(message: LogMessage) {
		// TODO: Perhaps don't just drop logs when we haven't
		// yet initialised (this happens when a device has not yet
		// been provisioned)
		if (this.unmanaged || !this.publishEnabled || !this.initialised) {
			return;
		}

		if (!_.isObject(message)) {
			return;
		}

		message = _.assign(
			{
				timestamp: Date.now(),
				message: '',
			},
			message,
		);

		if (!message.isSystem && message.serviceId == null) {
			return;
		}

		message.message = _.truncate(message.message, {
			length: MAX_LOG_LENGTH,
			omission: '[...]',
		});

		this.write(message);
	}

	public assignFields(apiEndpoint: string, uuid: string, deviceApiKey: string) {
		this.opts = url.parse(`${apiEndpoint}/device/v2/${uuid}/log-stream`) as any;
		this.opts.method = 'POST';
		this.opts.headers = {
			Authorization: `Bearer ${deviceApiKey}`,
			'Content-Type': 'application/x-ndjson',
			'Content-Encoding': 'gzip',
		};

		this.initialised = true;
	}

	private setup = _.throttle(() => {
		this.req = https.request(this.opts);

		// Since we haven't sent the request body yet, and never will,the
		// only reason for the server to prematurely respond is to
		// communicate an error. So teardown the connection immediately
		this.req.on('response', res => {
			log.error(
				'LogBackend: server responded with status code:',
				res.statusCode,
			);
			this.teardown();
		});

		this.req.on('timeout', () => this.teardown());
		this.req.on('close', () => this.teardown());
		this.req.on('error', err => {
			log.error('LogBackend: unexpected error:', err);
			this.teardown();
		});

		// Immediately flush the headers. This gives a chance to the server to
		// respond with potential errors such as 401 authentication error
		this.req.flushHeaders();

		// We want a very low writable high watermark to prevent having many
		// chunks stored in the writable queue of @_gzip and have them in
		// @_stream instead. This is desirable because once @_gzip.flush() is
		// called it will do all pending writes with that flush flag. This is
		// not what we want though. If there are 100 items in the queue we want
		// to write all of them with Z_NO_FLUSH and only afterwards do a
		// Z_SYNC_FLUSH to maximize compression
		this.gzip = zlib.createGzip({ writableHighWaterMark: 1024 });
		this.gzip.on('error', () => this.teardown());
		this.gzip.pipe(this.req);

		// Only start piping if there has been no error after the header flush.
		// Doing it immediately would potentialy lose logs if it turned out that
		// the server is unavailalbe because @_req stream would consume our
		// passthrough buffer
		this.timeout = setTimeout(() => {
			if (this.gzip != null) {
				this.stream.pipe(this.gzip);
				setImmediate(this.flush);
			}
		}, RESPONSE_GRACE_PERIOD);
	}, COOLDOWN_PERIOD);

	private snooze = _.debounce(this.teardown, KEEPALIVE_TIMEOUT);

	// Flushing every ZLIB_TIMEOUT hits a balance between compression and
	// latency. When ZLIB_TIMEOUT is 0 the compression ratio is around 5x
	// whereas when ZLIB_TIMEOUT is infinity the compession ratio is around 10x.
	private flush = _.throttle(
		() => {
			if (this.gzip != null) {
				this.gzip.flush(zlib.Z_SYNC_FLUSH);
			}
		},
		ZLIB_TIMEOUT,
		{ leading: false },
	);

	private teardown() {
		if (this.req != null) {
			clearTimeout(this.timeout);
			this.req.removeAllListeners();
			this.req.on('error', _.noop);
			if (this.gzip != null) {
				this.stream.unpipe(this.gzip);
				this.gzip.end();
			}
			this.req = null;
		}
	}

	private write(message: LogMessage) {
		this.snooze();
		if (this.req == null) {
			this.setup();
		}

		if (this.writable) {
			this.writable = this.stream.write(JSON.stringify(message) + '\n');
			this.flush();
		} else {
			this.dropCount += 1;
		}
	}
}
