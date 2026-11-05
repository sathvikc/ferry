// Streaming ZIP writer for files already on disk. Entries are stored (method 0), not compressed.
// CRC32 and size of every entry are known before streaming starts, so headers go out ahead of the data and the total length is exact.
// ZIP64 fields are added only where a size or offset passes 4 GiB.

const fs = require('node:fs');
const { pipeline } = require('node:stream/promises');

const MAX32 = 0xffffffff;
const UTF8_FLAG = 0x0800; // general purpose bit 11: names are UTF-8

// ---------- field helpers ----------

// DOS date/time packed into two 16-bit words; years before 1980 cannot be represented
function dosTime(ms) {
  const d = new Date(ms);

  return {
    time: (d.getHours() << 11) | (d.getMinutes() << 5) | (d.getSeconds() >> 1),
    date: ((Math.max(d.getFullYear(), 1980) - 1980) << 9) | ((d.getMonth() + 1) << 5) | d.getDate(),
  };
}

// ZIP64 extended-information extra field (id 0x0001) carrying only the given 64-bit values
function zip64Extra(values) {
  if (!values.length) return Buffer.alloc(0);

  const b = Buffer.alloc(4 + values.length * 8);
  b.writeUInt16LE(0x0001, 0);
  b.writeUInt16LE(values.length * 8, 2);
  values.forEach((v, i) => b.writeBigUInt64LE(BigInt(v), 4 + i * 8));

  return b;
}

// ---------- records ----------

// local file header, written right before each entry's data
function localHeader(e) {
  const big = e.size >= MAX32;
  const extra = zip64Extra(big ? [e.size, e.size] : []);
  const h = Buffer.alloc(30);

  h.writeUInt32LE(0x04034b50, 0);
  h.writeUInt16LE(big ? 45 : 20, 4);           // version needed
  h.writeUInt16LE(UTF8_FLAG, 6);
  h.writeUInt16LE(0, 8);                       // method: stored
  h.writeUInt16LE(e.dos.time, 10);
  h.writeUInt16LE(e.dos.date, 12);
  h.writeUInt32LE(e.crc, 14);
  h.writeUInt32LE(big ? MAX32 : e.size, 18);   // compressed size
  h.writeUInt32LE(big ? MAX32 : e.size, 22);   // uncompressed size
  h.writeUInt16LE(e.name.length, 26);
  h.writeUInt16LE(extra.length, 28);

  return Buffer.concat([h, e.name, extra]);
}

// central directory header, one per entry, all written after the last entry's data
function centralHeader(e) {
  const bigSize = e.size >= MAX32;
  const bigOffset = e.offset >= MAX32;
  const extra = zip64Extra([...(bigSize ? [e.size, e.size] : []), ...(bigOffset ? [e.offset] : [])]);
  const h = Buffer.alloc(46);

  h.writeUInt32LE(0x02014b50, 0);
  h.writeUInt16LE(45, 4);                              // version made by
  h.writeUInt16LE(bigSize || bigOffset ? 45 : 20, 6);  // version needed
  h.writeUInt16LE(UTF8_FLAG, 8);
  h.writeUInt16LE(0, 10);                              // method: stored
  h.writeUInt16LE(e.dos.time, 12);
  h.writeUInt16LE(e.dos.date, 14);
  h.writeUInt32LE(e.crc, 16);
  h.writeUInt32LE(bigSize ? MAX32 : e.size, 20);
  h.writeUInt32LE(bigSize ? MAX32 : e.size, 24);
  h.writeUInt16LE(e.name.length, 28);
  h.writeUInt16LE(extra.length, 30);
  // bytes 32-41 (comment length, disk number, attributes) stay 0
  h.writeUInt32LE(bigOffset ? MAX32 : e.offset, 42);

  return Buffer.concat([h, e.name, extra]);
}

// end of central directory, preceded by the ZIP64 record and locator when any count or offset overflows
function endRecords(count, cdSize, cdOffset) {
  const out = [];

  if (count >= 0xffff || cdSize >= MAX32 || cdOffset >= MAX32) {
    const r = Buffer.alloc(56);
    r.writeUInt32LE(0x06064b50, 0);
    r.writeBigUInt64LE(44n, 4);                  // size of the rest of this record
    r.writeUInt16LE(45, 12);
    r.writeUInt16LE(45, 14);
    r.writeBigUInt64LE(BigInt(count), 24);       // entries on this disk
    r.writeBigUInt64LE(BigInt(count), 32);       // entries total
    r.writeBigUInt64LE(BigInt(cdSize), 40);
    r.writeBigUInt64LE(BigInt(cdOffset), 48);

    const locator = Buffer.alloc(20);
    locator.writeUInt32LE(0x07064b50, 0);
    locator.writeBigUInt64LE(BigInt(cdOffset + cdSize), 8);
    locator.writeUInt32LE(1, 16);                // total disks

    out.push(r, locator);
  }

  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0);
  end.writeUInt16LE(Math.min(count, 0xffff), 8);
  end.writeUInt16LE(Math.min(count, 0xffff), 10);
  end.writeUInt32LE(Math.min(cdSize, MAX32), 12);
  end.writeUInt32LE(Math.min(cdOffset, MAX32), 16);
  out.push(end);

  return Buffer.concat(out);
}

// ---------- public API ----------

// entries: [{ path (file on disk), name (path inside the zip), size, crc, at }]
// returns the exact archive length and a function that streams the archive into a writable
function planZip(entries) {
  let offset = 0;

  const plan = entries.map(src => {
    const e = { ...src, name: Buffer.from(src.name, 'utf8'), dos: dosTime(src.at), offset };
    e.local = localHeader(e);
    offset += e.local.length + e.size;
    return e;
  });

  const central = Buffer.concat(plan.map(centralHeader));
  const tail = Buffer.concat([central, endRecords(plan.length, central.length, offset)]);
  const length = offset + tail.length;

  async function* bytes() {
    for (const e of plan) {
      yield e.local;
      yield* fs.createReadStream(e.path);
    }
    yield tail;
  }

  // pipeline handles backpressure and closes the open file if the client disconnects
  const pipeTo = out => pipeline(bytes, out);

  return { length, pipeTo };
}

module.exports = { planZip };
