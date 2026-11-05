// Compact id -> line-hash index for corpus deltas.
//
// A V8 Map holds at most 2^24 (16,777,216) entries, and country-scale shards
// exceed that (brazil ~67M docs, italy ~37M, texas/australia/florida ~24M).
// This open-addressing table lives in typed arrays instead: 64-bit id hash +
// 64-bit line hash + one state byte per slot, ~17 bytes/slot at <= 50% load,
// so brazil fits in ~2.3 GiB of off-heap memory with no entry cap.
//
// Ids are hashed, not stored. A 64-bit id collision (~1e-4 odds at 67M ids)
// can only re-emit an unchanged document or miscount one deletion; it never
// drops an added or changed document from the delta.

const EMPTY = 0;
const PRESENT = 1;
const SEEN = 2;
const MIN_CAPACITY = 1 << 10;

// cyrb53-style string hash, returning both 32-bit words.
export function hashId(id) {
  let h1 = 0xdeadbeef;
  let h2 = 0x41c6ce57;
  for (let i = 0; i < id.length; i++) {
    const ch = id.charCodeAt(i);
    h1 = Math.imul(h1 ^ ch, 2654435761);
    h2 = Math.imul(h2 ^ ch, 1597334677);
  }
  h1 = Math.imul(h1 ^ (h1 >>> 16), 2246822507) ^ Math.imul(h2 ^ (h2 >>> 13), 3266489909);
  h2 = Math.imul(h2 ^ (h2 >>> 16), 2246822507) ^ Math.imul(h1 ^ (h1 >>> 13), 3266489909);
  return [h1 >>> 0, h2 >>> 0];
}

export class CorpusDeltaIndex {
  constructor(capacity = MIN_CAPACITY) {
    let size = MIN_CAPACITY;
    while (size < capacity) size *= 2;
    this.#allocate(size);
    this.size = 0;
    this.seen = 0;
  }

  #allocate(capacity) {
    this.capacity = capacity;
    this.mask = capacity - 1;
    this.idHi = new Uint32Array(capacity);
    this.idLo = new Uint32Array(capacity);
    this.lineHi = new Uint32Array(capacity);
    this.lineLo = new Uint32Array(capacity);
    this.state = new Uint8Array(capacity);
  }

  #slot(idHi, idLo) {
    // idLo is already a well-mixed hash word; its low bits pick the slot.
    let slot = idLo & this.mask;
    while (this.state[slot] !== EMPTY) {
      if (this.idHi[slot] === idHi && this.idLo[slot] === idLo) return slot;
      slot = (slot + 1) & this.mask;
    }
    return slot;
  }

  #grow() {
    const { idHi, idLo, lineHi, lineLo, state } = this;
    this.#allocate(this.capacity * 2);
    for (let i = 0; i < state.length; i++) {
      if (state[i] === EMPTY) continue;
      const slot = this.#slot(idHi[i], idLo[i]);
      this.idHi[slot] = idHi[i];
      this.idLo[slot] = idLo[i];
      this.lineHi[slot] = lineHi[i];
      this.lineLo[slot] = lineLo[i];
      this.state[slot] = state[i];
    }
  }

  // Records a snapshot document. A repeated id keeps the last line hash,
  // matching the Map.set semantics this replaced.
  set(idHi, idLo, lineHi, lineLo) {
    if ((this.size + 1) * 2 > this.capacity) this.#grow();
    const slot = this.#slot(idHi, idLo);
    if (this.state[slot] === EMPTY) {
      this.size++;
      this.state[slot] = PRESENT;
      this.idHi[slot] = idHi;
      this.idLo[slot] = idLo;
    }
    this.lineHi[slot] = lineHi;
    this.lineLo[slot] = lineLo;
  }

  // Classifies a fresh document against the snapshot: "added" when the id is
  // unknown (or was already consumed by an earlier fresh line, as Map.delete
  // made it), else "same" / "changed" — consuming the snapshot entry.
  take(idHi, idLo, lineHi, lineLo) {
    const slot = this.#slot(idHi, idLo);
    if (this.state[slot] !== PRESENT) return "added";
    this.state[slot] = SEEN;
    this.seen++;
    return this.lineHi[slot] === lineHi && this.lineLo[slot] === lineLo ? "same" : "changed";
  }

  // Snapshot documents no fresh line consumed.
  get remaining() {
    return this.size - this.seen;
  }
}
