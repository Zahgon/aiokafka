import random


class DefaultPartitioner:

    @classmethod
    def __call__(cls, key, all_partitions, available):
        """
        Get the partition corresponding to key
        :param key: partitioning key
        :param all_partitions: list of all partitions sorted by partition ID
        :param available: list of available partitions in no particular order
        :return: one of the values from all_partitions or available
        """
        if key is None:
            if available:
                return random.choice(available)
            return random.choice(all_partitions)

        idx = murmur2(key)
        idx &= 0x7FFFFFFF
        idx %= len(all_partitions)
        return all_partitions[idx]


def murmur2(data):
    """Pure-python Murmur2 implementation.

    Based on java client, see org.apache.kafka.common.utils.Utils.murmur2

    Args:
        data (bytes): opaque bytes

    Returns: MurmurHash2 of data
    """
    length = len(data)
    seed = 0x9747B28C
    m = 0x5BD1E995
    r = 24

    h = seed ^ length
    length4 = length // 4

    for i in range(length4):
        i4 = i * 4
        k = (
            (data[i4 + 0] & 0xFF)
            + ((data[i4 + 1] & 0xFF) << 8)
            + ((data[i4 + 2] & 0xFF) << 16)
            + ((data[i4 + 3] & 0xFF) << 24)
        )
        k &= 0xFFFFFFFF
        k *= m
        k &= 0xFFFFFFFF
        k ^= (k % 0x100000000) >> r  # k ^= k >>> r
        k &= 0xFFFFFFFF
        k *= m
        k &= 0xFFFFFFFF

        h *= m
        h &= 0xFFFFFFFF
        h ^= k
        h &= 0xFFFFFFFF

    extra_bytes = length % 4
    if extra_bytes >= 3:
        h ^= (data[(length & ~3) + 2] & 0xFF) << 16
        h &= 0xFFFFFFFF
    if extra_bytes >= 2:
        h ^= (data[(length & ~3) + 1] & 0xFF) << 8
        h &= 0xFFFFFFFF
    if extra_bytes >= 1:
        h ^= data[length & ~3] & 0xFF
        h &= 0xFFFFFFFF
        h *= m
        h &= 0xFFFFFFFF

    h ^= (h % 0x100000000) >> 13  # h >>> 13;
    h &= 0xFFFFFFFF
    h *= m
    h &= 0xFFFFFFFF
    h ^= (h % 0x100000000) >> 15  # h >>> 15;
    h &= 0xFFFFFFFF

    return h
