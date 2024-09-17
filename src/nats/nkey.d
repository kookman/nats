module nats.nkey;

// implement NATS NKey signing, using ed25519.c to provide the ed25519 implementation

import std.string: representation;

class NkeyException : Exception {
    this(string message, string file = __FILE__, size_t line = __LINE__, Exception next = null) @safe
    {
        super(message, file, line, next);
    }
}

enum NATS_CRYPTO_SIGN_BYTES = 64;
enum NATS_CRYPTO_SECRET_BYTES = 64;

ubyte[] natsSign(in string encodedSeed, in ubyte[] input) @safe
{
    import std.exception: enforce;
    
    auto seed = decodeSeed(encodedSeed);
    auto key = extractKeyFromSeed(seed);
    auto signed = cryptoSign(input, key);
    auto check = secureZero(seed) + secureZero(key);
    enforce(check == 0, "Secrets not zeroed!");
    return signed;
}

int secureZero(ref ubyte[] secret) @trusted
{
    // we use C memset function here and return a checksum in an attempt to avoid this code
    // being elided by the compiler's optimisations
    import core.stdc.string: memset;
    import std.algorithm.iteration: sum;

    memset(secret.ptr, 0, secret.length);
    return sum(secret);
}

unittest {
    import std.algorithm.comparison: equal;
    import std.base64: Base64;
    import std.exception: assertThrown;

    // test extractKeyFromSeed
    auto encodedSeed = "SUAMLK2ZNL35WSMW37E7UD4VZ7ELPKW7DHC3BWBSD2GCZ7IUQQXZIORRBU";
    auto seed = decodeSeed(encodedSeed);
    assert(seed.length == 32, "Expected seed length of 32 bytes.");
    auto key = extractKeyFromSeed(seed);
    auto expectedKey = decodeBase32(
        "PDC2WWLK67NUTFW7ZH5A7FOPZC32VXYZYWYNQMQ6RQWP2FEEF6KDVFOW7W7PHAXEGS3QMBNABEYMCZHB6K4G2PAHSEZQKS4Q5JKTVVDCJORA");
    // ignore prefix and CRC bytes
    expectedKey = expectedKey[1..$-2];
    assert(key.equal(expectedKey), "Key extracted did not match expected.");

    // test natsSign
    auto signedNonce = Base64.decode(
        "ZaAiVDgB5CeYoXoQ7cBCmq+ZllzUnGUoDVb8C7PilWvCs8XKfUchAUhz2P4BYAF++Dg3w05CqyQFRDiGL6LrDw=="
    );
    auto nonce = "PXoWU7zWAMt75FY".representation;
    auto signed = natsSign(encodedSeed, nonce);
    assert(signed[0..$-nonce.length].equal(signedNonce), "Signed nonce failed to match.");  

    // Test invalid seed
    assertThrown!NkeyException(signed = natsSign("ABC", nonce), "Accepted dodgy seed!");
}


private:

ubyte[] decodeSeed(in string encodedSeed) @safe
{
    auto decodedSeed = decodeBase32(encodedSeed);
    if (decodedSeed.length < 4)
        throw new NkeyException("Invalid encoded key");

    // Read the crc that is stored as the two last bytes
    auto crcbytes = decodedSeed[$-2..$];
    ushort crc = cast(ushort)(crcbytes[0] | (crcbytes[1] << 8));

    // ensure checksum is valid
    if (!nats_CRC16_Validate(decodedSeed[0..$-2], crc))
        throw new NkeyException("Invalid checksum");

    // Need to do the reverse here to get back to internal representation.
    ubyte seedPrefix = decodedSeed[0] & 248;                                // 248 = 11111000
    ubyte seedTypePrefix = (decodedSeed[0] & 7) << 5 | ((decodedSeed[1] & 248) >> 3);  // 7 = 00000111

    if (seedPrefix != PREFIX_BYTE_SEED)
        throw new NkeyException("Invalid seed");

    if (!isValidPublicPrefixByte(seedTypePrefix))
        throw new NkeyException("Invalid prefix");
    
    return decodedSeed[2..$-2];
}

enum base32Alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

ubyte[] decodeBase32(scope string encoded) @safe
{
    import std.string: indexOf, stripRight;

    // Remove padding if present
    encoded = encoded.stripRight("=");

    ubyte[] result;
    size_t bitBuffer = 0;
    int bitBufferLen = 0;

    foreach (char c; encoded) {
        auto index = base32Alphabet.indexOf(c);
        if (index == -1)
            throw new Exception("Invalid character in base32 string");

        bitBuffer = (bitBuffer << 5) | index;
        bitBufferLen += 5;

        while (bitBufferLen >= 8) {
            bitBufferLen -= 8;
            result ~= cast(ubyte)((bitBuffer >> bitBufferLen) & 0xFF);
        }
    }

    return result;
}

unittest {
    import std.algorithm.comparison: equal;
    import std.string: representation;
    import std.exception: assertThrown;

    // Test case 1: Basic "Hello world" example
    string encoded = "JBSWY3DPEB3W64TMMQ======";
    auto expected = "Hello world".representation;
    ubyte[] result = decodeBase32(encoded);
    assert(result.equal(expected), "Test case 1 failed: 'Hello World' decoding");

    // Test case 2: Empty string should return an empty array
    encoded = "";
    expected = [];
    result = decodeBase32(encoded);
    assert(result == expected, "Test case 2 failed: Empty string decoding");

    // Test case 3: "foobar" in Base32
    encoded = "MZXW6YTBOI======";
    expected = [102, 111, 111, 98, 97, 114]; // "foobar"
    result = decodeBase32(encoded);
    assert(result == expected, "Test case 3 failed: 'foobar' decoding");

    // Test case 4: Test with padding in the middle (invalid)
    assertThrown(decodeBase32("JBSWY=3DPEB======"), 
        "Test case 4 failed: Invalid input with padding in the middle should throw");

    // Test case 5: Invalid character in input string
    // '@' is not a valid Base32 character
    assertThrown(decodeBase32("JBSWY3DP@B3W64TMMQ"),
        "Test case 5 failed: Invalid character should throw an exception");

    // Test case 6: Longer string and test zeroing buffer afterwards
    encoded = "KRUGS4ZANFZSA5DIMUQHEZLTOVWHIIDPMYQGCIDCMFZWKMZSEBSGKY3PMRUW4ZY";
    expected = "This is the result of a base32 decoding".representation;
    result = decodeBase32(encoded);
    auto beforePtr = result.ptr;
    assert(result.equal(expected), "Test case 6 failed - longer string");
    result[] = 0;
    assert(result.ptr == beforePtr, "Zeroing result buffer failed (caused reallocation)");
}

// PREFIX_BYTE_SEED is the version byte used for encoded NATS Seeds
enum ubyte PREFIX_BYTE_SEED = 18 << 3;     // Base32-encodes to 'S...';

// PREFIX_BYTE_PRIVATE is the version byte used for encoded NATS Private keys
enum ubyte PREFIX_BYTE_PRIVATE = 15 << 3;  // Base32-encodes to 'P...';

// PREFIX_BYTE_SERVER is the version byte used for encoded NATS Servers
enum ubyte PREFIX_BYTE_SERVER = 13 << 3;   // Base32-encodes to 'N...';

// PREFIX_BYTE_CLUSTER is the version byte used for encoded NATS Clusters
enum ubyte PREFIX_BYTE_CLUSTER = 2 << 3;   // Base32-encodes to 'C...';

// PREFIX_BYTE_ACCOUNT is the version byte used for encoded NATS Accounts
enum ubyte PREFIX_BYTE_ACCOUNT = 0;        // Base32-encodes to 'A...';

// PREFIX_BYTE_USER is the version byte used for encoded NATS Users
enum ubyte PREFIX_BYTE_USER = 20 << 3;     // Base32-encodes to 'U...'

bool isValidPublicPrefixByte(ubyte b) @safe
{
    switch (b)
    {
        case PREFIX_BYTE_USER:
        case PREFIX_BYTE_SERVER:
        case PREFIX_BYTE_CLUSTER:
        case PREFIX_BYTE_ACCOUNT:
            return true;
        default:
            return false;
    }
}

// Returns the 2-byte crc for the data provided.
ushort nats_CRC16_Compute(ubyte[] data) @safe
{
    ushort crc;

    foreach (ubyte_; data) {
        crc = ((crc << 8) & 0xFFFF) ^ crc16tab[((crc >> 8) ^ cast(ushort) ubyte_) & 0x00FF];
    }

    return crc;
}

// Checks the calculated crc16 checksum for data against the expected.
bool nats_CRC16_Validate(ubyte[] data, ushort expected) @safe
{
    ushort crc = nats_CRC16_Compute(data);
    return crc == expected;
}

unittest {
    ubyte[] testdata = [153, 209, 36, 74, 103, 32, 65, 34, 111, 68, 104, 156, 50, 14, 164, 140, 144, 230];
    ushort expected = 10272;

    ushort crc = nats_CRC16_Compute(testdata);
    assert(crc == expected);
    assert(nats_CRC16_Validate(testdata, expected));
    testdata[3] = 63;
    assert(!nats_CRC16_Validate(testdata, expected));
}

static immutable ushort[256] crc16tab = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
];

ubyte[] cryptoSign(in ubyte[] msg, const(ubyte[]) signingKey) @trusted
{
    // we need to pass signedMsglen as a pointer as reqd by sol_nacl API
    size_t signedMsglen = NATS_CRYPTO_SIGN_BYTES + msg.length;  
    auto signedMsg = new ubyte[signedMsglen];
    nkey_crypto_sign(signedMsg.ptr, &signedMsglen, msg.ptr, msg.length, signingKey.ptr);
    return signedMsg;
}

ubyte[] extractKeyFromSeed(in ubyte[] seed) @trusted
{
    auto key = new ubyte[NATS_CRYPTO_SECRET_BYTES];
    newKeyFromSeed(seed.ptr, key.ptr);
    return key;
}

// ed25519 crypto function declarations
extern(C) @nogc nothrow @trusted:
import ed25519;
void newKeyFromSeed(const(ubyte)* seed, ubyte* sk);
int nkey_crypto_sign(ubyte* sm, ulong* smlen_p, const(ubyte)* m, 
                        ulong mlen, const(ubyte)* sk);
