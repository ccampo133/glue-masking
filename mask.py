# This file contains reference implementations of masking functions that
# preserve the format of the input data consistently, such that the same
# input will always produce the same output. The functions are implemented
# in Python and can be used as UDFs in tools such as PySpark (e.g. with
# AWS Glue or Databricks).
import hashlib
import random


# consistent_mask is a reference implementation of a masking function that
# preserves the format of the input data consistently, such that the same
# input will always produce the same output. It replaces each alphanumeric
# character with a random character of the same type (uppercase, lowercase,
# or digit), while leaving all other characters unchanged.
def consistent_mask(data: str | int) -> str | int:
    rand = random.Random(data)
    resp = []
    unmasked = data if isinstance(data, str) else str(data)
    for char in unmasked:
        if '0' <= char <= '9':
            # 48 = ASCII code for '0', 57 = ASCII code for '9'.
            resp.append(chr(rand.randint(48, 57)))
        elif 'A' <= char <= 'Z':
            # 65 = ASCII code for 'A', 90 = ASCII code for 'Z'.
            resp.append(chr(rand.randint(65, 90)))
        elif 'a' <= char <= 'z':
            # 97 = ASCII code for 'a', 122 = ASCII code for 'z'.
            resp.append(chr(rand.randint(97, 122)))
        else:
            resp.append(char)
    masked = "".join(resp)
    return masked if isinstance(data, str) else int(masked)


# consistent_mask_hash is a reference implementation of a masking function that
# preserves the format of the input data consistently, such that the same
# input will always produce the same output. It replaces each alphanumeric
# character with a character of the same type (uppercase, lowercase, or digit),
# while leaving all other characters unchanged. The main difference from
# consistent_mask is that it uses the SHA-256 hash function to generate the
# "random" characters, as opposed to a PRNG. This makes it portable across
# systems, as it does not rely on the system's random number generator
# implementation. This implementation matches the behavior of the various
# consistent_mask_hash SQL UDFs in the other files and will produce the same
# output for the same input.
def consistent_mask_hash(data: str | int) -> str | int:
    resp = []
    unmasked = data if isinstance(data, str) else str(data)
    # Explanation of the magic numbers below:
    # 10 is the number of digits (0-9).
    # 48 is the ASCII code for '0'.
    # 26 is the number of letters in the ISO Latin alphabet.
    # 65 is the ASCII code for 'A'.
    # 97 is the ASCII code for 'a'.
    for i, char in enumerate(unmasked):
        if char < '0' or char > 'z':
            resp.append(char)
        else:
            hash_bytes = hashlib.sha256((str(i) + data).encode()).digest()
            # Use the first 4 bytes of the hash as a "random" number.
            rand = int.from_bytes(hash_bytes[:4], byteorder='big')
            if '0' <= char <= '9':
                resp.append(chr(rand % 10 + 48))
            if 'A' <= char <= 'Z':
                resp.append(chr(rand % 26 + 65))
            elif 'a' <= char <= 'z':
                resp.append(chr(rand % 26 + 97))
    masked = "".join(resp)
    return masked if isinstance(data, str) else int(masked)
