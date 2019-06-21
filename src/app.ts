// Make NodeJS RFC 3484 compliant for properly handling IPv6
// See: https://github.com/nodejs/node/pull/14731
//      https://github.com/nodejs/node/pull/17793
import * as dns from 'dns';
const { lookup } = dns;

// Lookup is readonly property, so we tell typescript to
// ignore the following line
// @ts-ignore
dns.lookup = (name: string, opts: any, cb: (err: Error | null) => void) => {
	if (typeof cb !== 'function') {
		return lookup(name, { verbatim: true }, opts);
	}
	return lookup(name, Object.assign({ verbatim: true }, opts), cb);
};

import Supervisor from './supervisor';

const supervisor = new Supervisor();
supervisor.init();
