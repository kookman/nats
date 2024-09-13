module nats.nuid;

// this module is a port of the golang version at https://github.com/nats-io/nuid

// given that Dlang has TLS and std.random's rndGen uses an unpredictableSeed by default,
// locking with a mutex (which is present in the original Go version) is not required
// to ensure unique NUIDs.
// there is a test in the main function to validate any (trivial) flaws in this assumption

import std.random;
import core.sync.mutex;

enum digits = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
enum base = 62;
enum preLen = 12;
enum seqLen = 10;
enum maxSeq = 839299365868340224L; // base^seqLen == 62^10
enum minInc = 33L;
enum maxInc = 333L;
enum totalLen = preLen + seqLen;

struct NUID {
    private ubyte[preLen] pre;
    private long seq;
    private long inc;
    private Random rnd;

    this(Random generator) {
        rnd = generator;
        resetSequential();
        randomizePrefix();
    }

    void randomizePrefix() {
        foreach (i; 0 .. preLen) {
            pre[i] = cast(ubyte) digits[rnd.front % base];
            rnd.popFront();
        }
    }

    void resetSequential() {
        seq = rnd.front % maxSeq;
        rnd.popFront();
        inc = minInc + (rnd.front % (maxInc - minInc));
        rnd.popFront();
    }

    string next() {
        seq += inc;
        if (seq >= maxSeq) {
            randomizePrefix();
            resetSequential();
        }

        char[totalLen] b;
        b[0 .. preLen] = cast(char[]) pre;

        long l = seq;
        for (int i = totalLen - 1; i >= preLen; i--) {
            b[i] = digits[l % base];
            l /= base;
        }

        return b.idup;
    }
}

unittest {
    import std.array : array;
    import std.conv : to;
    import std.algorithm.comparison: equal;

    // TestDigits
    assert(digits.length == base, "digits length does not match base modulo");

    // TestNUIDRollover
    tlsNUID.seq = maxSeq;
    auto oldPre = tlsNUID.pre.array;
    next();
    assert(!equal(tlsNUID.pre[], oldPre), "Expected new pre, got the old one");

    // TestGUIDLen
    auto nuid = next();
    assert(nuid.length == totalLen, "Expected len of " ~ to!string(totalLen) ~ ", got " ~ to!string(nuid.length));

    // TestProperPrefix
    ubyte min = 255;
    ubyte max = 0;
    foreach (d; digits) {
        if (d < min) min = d;
        if (d > max) max = d;
    }
    int total = 100000;
    foreach (i; 0 .. total) {
        auto n = NUID(rndGen());
        foreach (j; 0 .. preLen) {
            assert(n.pre[j] >= min && n.pre[j] <= max, "Iter " ~ to!string(i) ~ ". Valid range for bytes prefix: [" ~ to!string(min) ~ ".." ~ to!string(max) ~ "]\nIncorrect prefix at pos " ~ to!string(j) ~ ": " ~ to!string(n.pre));
        }
    }
}

struct LockedNUID {
    private NUID nuid;
    private Mutex mutex;

    this(Random generator) {
        nuid = NUID(generator);
        mutex = new Mutex();
    }

    string next() {
        synchronized (mutex) {
            return nuid.next();
        }
    }
}

private NUID tlsNUID;

static this() {
    tlsNUID = NUID(rndGen());
}

string next() {
    return tlsNUID.next();
}

version (Have_vibe_core) {}
else {
    // main() is only defined if building module on its own.
    // run tests for benchmarking NUID generation and checking uniqueness over many samples. 
    // These tests are too expensive to include in normal unittest run of whole Nats client.
    void main() {
        import std.datetime.stopwatch : benchmark;
        import std.parallelism: parallel;
        import std.exception: enforce;
        import std.stdio : writeln;

        enum n = 10_000_000;
        auto nuid = NUID(rndGen());
        auto duration = benchmark!(() => nuid.next())(n);
        writeln("Generated ", n, " NUIDs in ", duration[0].total!"usecs", " microseconds");

        //for comparison, here is LockedNUID version:
        auto locked = LockedNUID(rndGen());
        duration = benchmark!(() => locked.next())(n);
        writeln("LockedNUID generated ", n, " NUIDs in ", duration[0].total!"usecs", " microseconds");

        // TestBasicUniqueness
        auto m = new bool[string];
        string[] ids;
        ids.reserve(n);
        writeln("Generating ", n, " NUIDs for single thread uniqueness test...");
        foreach (i; 0 .. n) {
            ids ~= next();
        }
        writeln("Testing uniqueness...");
        foreach (id; ids) {
            enforce(!(id in m), "Duplicate ID found!: " ~ id);
            m[id] = true;
        }
        writeln("Passed.\n");
        
        // more intensive parallel test
        ids = new string[n * 4];
        writeln("Generating ", n*4, " NUIDs for parallel uniqueness test...");
        foreach (ref id; parallel(ids)) {
            id = next();
        }
        writeln("Testing uniqueness...");
        m.clear;
        foreach (id; ids) {
            enforce(!(id in m), "Duplicate ID found in parallel test!: " ~ id);
            m[id] = true;
        }
        writeln("Passed.\n");
    }
}